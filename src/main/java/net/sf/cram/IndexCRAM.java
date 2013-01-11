package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.BAMIndexer;
import net.sf.samtools.CRAMFileReader;
import net.sf.samtools.CRAMIndexer;
import net.sf.samtools.SAMException;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.CloseableIterator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class IndexCRAM {
	private static Log log = Log.getInstance(IndexCRAM.class);

	private CountingInputStream is;
	private SAMFileHeader samFileHeader;
	private CRAMIndexer indexer;

	public IndexCRAM(InputStream is, SAMFileHeader samFileHeader, File output) {
		this.is = new CountingInputStream(is);
		this.samFileHeader = samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	public IndexCRAM(InputStream is, File output) throws IOException {
		this.is = new CountingInputStream(is);
		CramHeader cramHeader = ReadWrite.readCramHeader(this.is);
		samFileHeader = cramHeader.samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	private void nextContainer() throws IOException {
		long offset = is.getCount();
		Container c = ReadWrite.readContainer(samFileHeader, is);
		c.offset = offset;

		int i = 0;
		for (Slice slice : c.slices) {
			slice.containerOffset = offset;
			slice.index = i++;
			indexer.processAlignment(slice);
		}

		log.info("INDEXED: " + c.toString());
	}

	private void index() throws IOException {
		while (true) {
			try {
				nextContainer();
			} catch (EOFException e) {
				break;
			}
		}
	}

	public void run() throws IOException {
		index();
		indexer.finish();
	}

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
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

		if (params.referenceFastaFile == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		if (CRAMFileReader.isCRAMFile(params.inputFile)) {
			File cramIndexFile = new File(params.inputFile.getAbsolutePath()
					+ ".bai");

			indexCramFile(params.inputFile, cramIndexFile,
					params.referenceFastaFile);

			if (params.test)
				randomTest(params.inputFile, cramIndexFile,
						params.referenceFastaFile, params.testMinPos,
						params.testMaxPos, params.testCount);
		} else {
			SAMFileReader reader = new SAMFileReader(params.inputFile);
			if (!reader.isBinary()) {
				reader.close();
				throw new SAMException(
						"Input file must be bam file, not sam file.");
			}

			if (!reader.getFileHeader().getSortOrder()
					.equals(SAMFileHeader.SortOrder.coordinate)) {
				reader.close();
				throw new SAMException(
						"Input bam file must be sorted by coordinates");
			}

			File indexFile = new File(params.inputFile.getAbsolutePath()
					+ ".bai");
			BAMIndexer indexer = new BAMIndexer(indexFile,
					reader.getFileHeader());
			for (SAMRecord record : reader) {
				indexer.processAlignment(record);
			}
			indexer.finish();
			reader.close();
		}

	}

	@Parameters(commandDescription = "BAM/CRAM indexer. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM or CRAM file to be indexed. Omit if standard input (pipe).")
		File inputFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFastaFile;

		@Parameter(names = { "--help", "-h" }, description = "Print help and exit.")
		boolean help = false;

		@Parameter(names = { "--test" }, hidden = true, description = "Random test of the built index.")
		boolean test = false;

		@Parameter(names = { "--test-min-pos" }, hidden = true, description = "Minimum alignment start for randomt test.")
		int testMinPos = 1;

		@Parameter(names = { "--test-max-pos" }, hidden = true, description = "Maximum alignment start for randomt test.")
		int testMaxPos = 100000000;

		@Parameter(names = { "--test-count" }, hidden = true, description = "Run random test this many times.")
		int testCount = 100;
	}

	public static void indexCramFile(File cramFile, File cramIndexFile,
			File refFile) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		IndexCRAM ic = new IndexCRAM(is, cramIndexFile);

		ic.run();
	}

	/**
	 * @param cramFile
	 * @param cramIndexFile
	 * @param refFile
	 * @param posMin
	 * @param posMax
	 * @param repeat
	 * @return the overhead, the number of records skipped before reached the
	 *         query or -1 if nothing was found.
	 */
	private static int randomTest(File cramFile, File cramIndexFile,
			File refFile, int posMin, int posMax, int repeat) {
		CRAMFileReader reader = new CRAMFileReader(cramFile, cramIndexFile,
				ReferenceSequenceFileFactory.getReferenceSequenceFile(refFile));

		int overhead = 0;

		Random random = new Random();
		for (int i = 0; i < repeat; i++) {
			int result = 0;
			int pos = random.nextInt(posMax - posMin) + posMin;
			try {
				result = query(reader, pos);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(String.format("Query failed at %d.", pos));
			}
			if (result > -1)
				overhead += repeat;
		}
		return overhead;
	}

	private static int query(CRAMFileReader reader, int position) {
		long timeStart = System.nanoTime();

		CloseableIterator<SAMRecord> iterator = reader.queryAlignmentStart(
				"20", position);

		SAMRecord record = null;
		int overhead = 0;
		while (iterator.hasNext()) {
			record = iterator.next();
			if (record.getAlignmentStart() >= position)
				break;
			else
				record = null;
			overhead++;
		}
		iterator.close();

		long timeStop = System.nanoTime();
		if (record == null)
			log.info(String.format("Query not found: position=%d, time=%dms.",
					position, (timeStop - timeStart) / 1000000));
		else
			log.info(String.format(
					"Query found: position=%d, overhead=%d, time=%dms.",
					position, overhead, (timeStop - timeStart) / 1000000));

		return overhead;
	}
}
