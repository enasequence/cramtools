package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTextWriter;
import net.sf.samtools.util.SeekableFileStream;
import uk.ac.ebi.embl.ega_cipher.CipherInputStream_256;
import uk.ac.ebi.embl.ega_cipher.SeekableCipherStream_256;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Cram2Bam {
	private static Log log = Log.getInstance(Cram2Bam.class);

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Cram2Bam.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out
					.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		if (params.reference == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		char[] pass = null;
		if (params.decrypt) {
			if (System.console() == null)
				throw new RuntimeException("Cannot access console.");
			pass = System.console().readPassword();
		}

		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(params.reference);

		InputStream is;
		if (params.cramFile != null) {
			FileInputStream fis = new FileInputStream(params.cramFile);
			is = new BufferedInputStream(fis);
		} else
			is = System.in;

		if (params.decrypt) {
			CipherInputStream_256 cipherInputStream_256 = new CipherInputStream_256(
					is, pass, 128);
			is = cipherInputStream_256.getCipherInputStream();
			// is = new SeekableCipherStream_256(new SeekableFileStream(
			// params.cramFile), pass, 1, 128);
		}

		CramHeader cramHeader = ReadWrite.readCramHeader(is);
		SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();
		samFileWriterFactory.setAsyncOutputBufferSize(100000);
		samFileWriterFactory.setCreateIndex(false);
		samFileWriterFactory.setCreateMd5File(false);
		samFileWriterFactory.setUseAsyncIo(true);

		SAMFileWriter writer = null;
		{ // building sam writer, sometimes we have to go deeper to get to the
			// required functionality:
			if (params.outputFile == null) {
				if (params.outputBAM) {
					BAMFileWriter ret = new BAMFileWriter(System.out, null);
					ret.setSortOrder(cramHeader.samFileHeader.getSortOrder(),
							true);
					ret.setHeader(cramHeader.samFileHeader);
					writer = ret;
				} else {
					if (params.printSAMHeader) {
						writer = samFileWriterFactory.makeSAMWriter(
								cramHeader.samFileHeader, true, System.out);
					} else {
						SwapOutputStream sos = new SwapOutputStream();

						final SAMTextWriter ret = new SAMTextWriter(sos);
						ret.setSortOrder(
								cramHeader.samFileHeader.getSortOrder(), true);
						ret.setHeader(cramHeader.samFileHeader);
						ret.getWriter().flush();

						writer = ret;

						sos.delegate = System.out;
					}
				}
			} else {
				writer = samFileWriterFactory.makeSAMOrBAMWriter(
						cramHeader.samFileHeader, true, params.outputFile);
			}
		}

		while (true) {
			Container c = null;
			try {
				c = ReadWrite.readContainer(cramHeader.samFileHeader, is);
			} catch (EOFException e) {
				break;
			}

			List<CramRecord> cramRecords = null;
			try {
				cramRecords = BLOCK_PROTO.getRecords(c.h, c,
						cramHeader.samFileHeader);
			} catch (EOFException e) {
				throw e;
			}

			byte[] ref = null;
			if (c.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				SAMSequenceRecord sequence = cramHeader.samFileHeader
						.getSequence(c.sequenceId);
				ReferenceSequence referenceSequence = referenceSequenceFile
						.getSequence(sequence.getSequenceName());
				ref = referenceSequence.getBases();
			}

			long time1 = System.nanoTime();
			CramNormalizer n = new CramNormalizer(cramHeader.samFileHeader,
					ref, c.alignmentStart);
			n.normalize(cramRecords, true);
			long time2 = System.nanoTime();

			Cram2BamRecordFactory c2sFactory = new Cram2BamRecordFactory(
					cramHeader.samFileHeader);

			long c2sTime = 0;
			long sWriteTime = 0;

			for (CramRecord r : cramRecords) {
				long time = System.nanoTime();
				SAMRecord s = c2sFactory.create(r);
				if (ref != null)
					Utils.calculateMdAndNmTags(s, ref, params.calculateMdTag,
							params.calculateNmTag);
				c2sTime += System.nanoTime() - time;
				try {

					time = System.nanoTime();
					writer.addAlignment(s);
					sWriteTime += System.nanoTime() - time;
					if (params.outputFile == null && System.out.checkError())
						break;
				} catch (NullPointerException e) {
					System.out.println(r.toString());
					throw e;
				}
			}

			log.info(String
					.format("CONTAINER READ: io %dms, parse %dms, norm %dms, convert %dms, BAM write %dms",
							c.readTime / 1000000, c.parseTime / 1000000,
							c2sTime / 1000000, (time2 - time1) / 1000000,
							sWriteTime / 1000000));

			if (params.outputFile == null && System.out.checkError())
				break;

		}

		writer.close();
	}

	private static class SwapOutputStream extends OutputStream {
		OutputStream delegate;

		@Override
		public void write(byte[] b) throws IOException {
			if (delegate != null)
				delegate.write(b);
		}

		@Override
		public void write(int b) throws IOException {
			if (delegate != null)
				delegate.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (delegate != null)
				delegate.write(b, off, len);
		}
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "--input-cram-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM file to uncompress. Omit if standard input (pipe).")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "--output-bam-file", "-O" }, converter = FileConverter.class, description = "The path to the output BAM file.")
		File outputFile;

		@Parameter(names = { "-b", "--output-bam-format" }, description = "Output in BAM format.")
		boolean outputBAM = false;

		@Parameter(names = { "--print-sam-header" }, description = "Print SAM header when writing SAM format.")
		boolean printSAMHeader = false;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--default-quality-score" }, description = "Use this quality score (decimal representation of ASCII symbol) as a default value when the original quality score was lost due to compression. Minimum is 33.")
		int defaultQS = '?';

		@Parameter(names = { "--calculate-md-tag" }, description = "Calculate MD tag.")
		boolean calculateMdTag = false;

		@Parameter(names = { "--calculate-nm-tag" }, description = "Calculate NM tag.")
		boolean calculateNmTag = false;

		@Parameter()
		List<String> locations;

		@Parameter(names = { "--decrypt" }, description = "Decrypt the file.")
		boolean decrypt = false;

	}

}
