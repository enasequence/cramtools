package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.Index.Entry;
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
import net.sf.samtools.util.SeekableStream;
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

		if (params.locations == null)
			params.locations = new ArrayList<String>();

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
			if (params.locations == null || params.locations.isEmpty())
				is = new BufferedInputStream(fis);
			else
				is = new SeekableFileStream(params.cramFile);
		} else
			is = System.in;

		if (params.decrypt) {
			CipherInputStream_256 cipherInputStream_256 = new CipherInputStream_256(
					is, pass, 128);
			is = cipherInputStream_256.getCipherInputStream();
			if (params.locations != null && !params.locations.isEmpty()) {
				is = new SeekableCipherStream_256(new SeekableFileStream(
						params.cramFile), pass, 1, 128);
			}
		}

		CramHeader cramHeader = ReadWrite.readCramHeader(is);

		List<Index.Entry> entries = null;
		if (!params.locations.isEmpty() && params.cramFile != null) {
			if (params.locations.size() > 1)
				throw new RuntimeException("Only one location is supported.");

			File indexFile = new File(params.cramFile.getAbsolutePath()
					+ ".crai");
			FileInputStream fis = new FileInputStream(indexFile);
			GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(
					fis));
			BufferedInputStream bis = new BufferedInputStream(gis);
			List<Index.Entry> full = Index.readIndex(gis);

			entries = new LinkedList<Index.Entry>();
			for (String location : params.locations) {
				String[] chunks = location.split(":");
				if (chunks.length != 2)
					throw new RuntimeException("Invalid location: " + location);
				String seq = chunks[0];
				SAMSequenceRecord sequence = cramHeader.samFileHeader
						.getSequence(seq);
				if (sequence == null)
					throw new RuntimeException("Sequence not found: " + seq);

				chunks = chunks[1].split("-");
				int start = 0, span = Integer.MAX_VALUE;
				if (chunks.length == 0)
					throw new RuntimeException("Invalid location: " + location);
				if (chunks.length > 1)
					start = Integer.valueOf(chunks[0]);
				if (chunks.length == 2)
					span = Integer.valueOf(chunks[1]) - start;
				if (chunks.length > 2)
					throw new RuntimeException("Invalid location: " + location);

				entries.addAll(Index.find(full, sequence.getSequenceIndex(),
						start, span));
			}

			if (entries.isEmpty()) {
				log.warn("No records found.");
				return;
			}

			bis.close();
		} else
			entries = new LinkedList<Index.Entry>();

		SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();
		samFileWriterFactory.setAsyncOutputBufferSize(100000);
		samFileWriterFactory.setCreateIndex(false);
		samFileWriterFactory.setCreateMd5File(false);
		samFileWriterFactory.setUseAsyncIo(true);

		SAMFileWriter writer = createSAMFileWriter(params, cramHeader,
				samFileWriterFactory);

		if (!entries.isEmpty()) {
			// position the stream for random access:
			if (is instanceof SeekableStream) {
				SeekableStream ss = (SeekableStream) is;
				Entry entry = entries.get(0);
				ss.seek(entry.offset);
			} else
				throw new RuntimeException(
						"The input stream does not support random access.");
		}

		long recordCount = 0;
		long readTime = 0;
		long parseTime = 0;
		long normTime = 0;
		long samTime = 0;
		long writeTime = 0;
		long time = 0;
		ArrayList<CramRecord> cramRecords = new ArrayList<CramRecord>(100000);

		CramNormalizer n = new CramNormalizer(cramHeader.samFileHeader);

		byte[] ref = null;
		int prevSeqId = -1;
		while (true) {
			Container c = null;
			try {
				time = System.nanoTime();
				c = ReadWrite.readContainer(cramHeader.samFileHeader, is);
				readTime += System.nanoTime() - time;
			} catch (EOFException e) {
				break;
			}

			if (params.countOnly && params.requiredFlags == 0
					&& params.filteringFlags == 0) {
				recordCount += c.nofRecords;
				continue;
			}

			try {
				time = System.nanoTime();
				cramRecords.clear();
				BLOCK_PROTO.getRecords(c.h, c, cramHeader.samFileHeader,
						cramRecords);
				parseTime += System.nanoTime() - time;
			} catch (EOFException e) {
				throw e;
			}

			if (c.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				ref = new byte[] {};
			} else if (prevSeqId < 0 || prevSeqId != c.sequenceId) {
				SAMSequenceRecord sequence = cramHeader.samFileHeader
						.getSequence(c.sequenceId);
				ReferenceSequence referenceSequence = referenceSequenceFile
						.getSequence(sequence.getSequenceName());
				ref = referenceSequence.getBases();
				prevSeqId = c.sequenceId;
			}

			long time1 = System.nanoTime();
			n.normalize(cramRecords, true, ref, c.alignmentStart);
			long time2 = System.nanoTime();
			normTime += time2 - time1;

			Cram2BamRecordFactory c2sFactory = new Cram2BamRecordFactory(
					cramHeader.samFileHeader);

			long c2sTime = 0;
			long sWriteTime = 0;

			for (CramRecord r : cramRecords) {
				time = System.nanoTime();
				SAMRecord s = c2sFactory.create(r);

				if (params.requiredFlags != 0
						&& ((params.requiredFlags & s.getFlags()) == 0))
					continue;
				if (params.filteringFlags != 0
						&& ((params.filteringFlags & s.getFlags()) != 0))
					continue;
				if (params.countOnly) {
					recordCount++;
					continue;
				}
				
				if (ref != null)
					Utils.calculateMdAndNmTags(s, ref, params.calculateMdTag,
							params.calculateNmTag);
				c2sTime += System.nanoTime() - time;
				samTime += System.nanoTime() - time;

				time = System.nanoTime();
				writer.addAlignment(s);
				sWriteTime += System.nanoTime() - time;
				writeTime += System.nanoTime() - time;
				if (params.outputFile == null && System.out.checkError())
					break;
			}

			// if (!params.countOnly) {
			// time = System.nanoTime();
			// for (SAMRecord s : samRecords) {
			// writer.addAlignment(s);
			// if (params.outputFile == null && System.out.checkError())
			// break;
			// }
			// sWriteTime += System.nanoTime() - time;
			// writeTime += System.nanoTime() - time;
			// }

			log.info(String
					.format("CONTAINER READ: io %dms, parse %dms, norm %dms, convert %dms, BAM write %dms",
							c.readTime / 1000000, c.parseTime / 1000000,
							(time2 - time1) / 1000000, c2sTime / 1000000,
							sWriteTime / 1000000));

			if (params.outputFile == null && System.out.checkError())
				break;

		}

		if (params.countOnly)
			System.out.println(recordCount);

		writer.close();

		log.warn(String
				.format("TIMES: io %ds, parse %ds, norm %ds, convert %ds, BAM write %ds",
						readTime / 1000000000, parseTime / 1000000000,
						normTime / 1000000000, samTime / 1000000000,
						writeTime / 1000000000));
	}

	private static SAMFileWriter createSAMFileWriter(Params params,
			CramHeader cramHeader, SAMFileWriterFactory samFileWriterFactory)
			throws IOException {
		/*
		 * building sam writer, sometimes we have to go deeper to get to the
		 * required functionality:
		 */
		SAMFileWriter writer = null;
		if (params.outputFastq) {
			if (params.cramFile == null) {
				writer = new FastqSAMFileWriter(System.out, null,
						cramHeader.samFileHeader);
			} else {
				writer = new FastqSAMFileWriter(
						params.cramFile.getAbsolutePath(), false,
						cramHeader.samFileHeader);

			}
		} else if (params.outputFastqGz) {
			if (params.cramFile == null) {
				GZIPOutputStream gos = new GZIPOutputStream(System.out);
				PrintStream ps = new PrintStream(gos);
				writer = new FastqSAMFileWriter(ps, null,
						cramHeader.samFileHeader);
			} else {
				writer = new FastqSAMFileWriter(
						params.cramFile.getAbsolutePath(), true,
						cramHeader.samFileHeader);

			}
		} else if (params.outputFile == null) {
			if (params.outputBAM) {
				BAMFileWriter ret = new BAMFileWriter(System.out, null);
				ret.setSortOrder(cramHeader.samFileHeader.getSortOrder(), true);
				ret.setHeader(cramHeader.samFileHeader);
				writer = ret;
			} else {
				if (params.printSAMHeader) {
					writer = samFileWriterFactory.makeSAMWriter(
							cramHeader.samFileHeader, true, System.out);
				} else {
					SwapOutputStream sos = new SwapOutputStream();

					final SAMTextWriter ret = new SAMTextWriter(sos);
					ret.setSortOrder(cramHeader.samFileHeader.getSortOrder(),
							true);
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
		return writer;
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

		@Parameter(names = { "-q", "--output-fastq-format" }, hidden = true, description = "Output in fastq format.")
		boolean outputFastq = false;

		@Parameter(names = { "-z", "--output-fastq-gz-format" }, hidden = true, description = "Output in gzipped fastq format.")
		boolean outputFastqGz = false;

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

		@Parameter(names = { "--count-only", "-c" }, description = "Count number of records.")
		boolean countOnly = false;

		@Parameter(names = { "--required-flags", "-f" }, description = "Required flags. ")
		int requiredFlags = 0;

		@Parameter(names = { "--filter-flags", "-F" }, description = "Filtering flags. ")
		int filteringFlags = 0;

	}

}
