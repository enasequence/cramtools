package htsjdk.samtools.cram.encoding.reader;

import java.io.File;
import java.util.zip.Deflater;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.Log;
import net.sf.cram.Cram2Fastq;
import net.sf.cram.CramTools.LevelConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class BAMNameCollate extends NameCollate<BAMRead> {
	private SAMFileWriter[] writers;
	private SAMFileWriter overspillWriter;
	private long kicked = 0, ready = 0, total = 0;

	public BAMNameCollate() {
		super();
	}

	public BAMNameCollate(SAMFileWriter[] writers, SAMFileWriter overspillWriter) {
		super();
		this.writers = writers;
		this.overspillWriter = overspillWriter;
	}

	@Override
	protected boolean needsCollating(BAMRead read) {
		return read.getRecord().getReadPairedFlag();
	}

	public int getStreamIndex(SAMRecord record) {
		if (!record.getReadPairedFlag())
			return 0;
		if (record.getFirstOfPairFlag())
			return 1;
		if (record.getSecondOfPairFlag())
			return 2;

		return 0;
	}

	@Override
	protected void ready(BAMRead read) {
		int streamIndex = getStreamIndex(read.getRecord());
		writers[streamIndex].addAlignment(read.getRecord());
		ready++;
	}

	@Override
	protected void kickedFromCache(BAMRead read) {
		overspillWriter.addAlignment(read.getRecord());
		kicked++;
	}

	@Override
	public void add(BAMRead read) {
		super.add(read);
		total++;
	}

	public String report() {
		return String.format("ready: %d, kicked %d, total: %d.", ready, kicked, total);
	}

	private static Log log = Log.getInstance(BAMNameCollate.class);
	public static final String COMMAND = "fastq";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Cram2Fastq.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws Exception {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		long maxRecords = params.maxRecords;

		File overspillFile = new File(params.file.getAbsolutePath().replaceAll("\\.bam$", ".tmp.bam"));
		File file0 = new File(params.file.getAbsolutePath().replaceAll("\\.bam$", ".read0.bam"));
		File file1 = new File(params.file.getAbsolutePath().replaceAll("\\.bam$", ".read1.bam"));
		File file2 = new File(params.file.getAbsolutePath().replaceAll("\\.bam$", ".read2.bam"));

		SAMFileReader reader = new SAMFileReader(params.file);
		reader.getFileHeader().setSortOrder(SAMFileHeader.SortOrder.unsorted);

		BAMNameCollate collate = new BAMNameCollate();
		collate.writers = new SAMFileWriter[3];
		if (params.compression < Deflater.NO_COMPRESSION || params.compression > Deflater.BEST_COMPRESSION) {
			System.err.println("Invalid compression level, expecting an integer from 0 to 9. ");
			System.exit(1);
		}
		BlockCompressedOutputStream.setDefaultCompressionLevel(params.compression);
		SAMFileWriterFactory writerFactory = new SAMFileWriterFactory();
		writerFactory.setUseAsyncIo(true);
		writerFactory.setAsyncOutputBufferSize(1024 * 100);
		collate.writers[0] = writerFactory.makeBAMWriter(reader.getFileHeader(), false, file0);
		collate.writers[1] = writerFactory.makeBAMWriter(reader.getFileHeader(), false, file1);
		collate.writers[2] = writerFactory.makeBAMWriter(reader.getFileHeader(), false, file2);
		collate.overspillWriter = writerFactory.makeBAMWriter(reader.getFileHeader(), false, overspillFile);

		long time = System.currentTimeMillis();
		long total = 0;
		for (SAMRecord record : reader) {
			if (maxRecords > -1) {
				if (maxRecords == 0)
					break;
				maxRecords--;
			}
			collate.add(new BAMRead(record));
			total++;
			if (System.currentTimeMillis() - time > 10 * 1000) {
				time = System.currentTimeMillis();
				System.out.println(collate.report());
			}
		}
		System.out.println("Read total records: " + total);
		reader.close();
		collate.close();
		collate.overspillWriter.close();

		File sorted = new File("overspill_sorted.bam");

		{ // sorting overfill:
			reader = new SAMFileReader(overspillFile);
			SAMFileHeader header = reader.getFileHeader().clone();
			header.setSortOrder(SAMFileHeader.SortOrder.queryname);
			SAMFileWriter writer = writerFactory.makeBAMWriter(header, false, sorted);

			long overfill = 0;
			System.out.println("Sorting overfill...");
			for (SAMRecord record : reader) {
				writer.addAlignment(record);
				overfill++;
			}

			writer.close();
			reader.close();
			System.out.println("Sorted records: " + overfill);
		}

		{
			reader = new SAMFileReader(sorted);
			SAMRecordIterator iterator = reader.iterator();
			long aftersort = 0;
			long aftersortMatch = 0;
			System.out.println("Sorting overfill...");
			SAMRecord r1 = iterator.next();
			SAMRecord r2 = null;
			while (iterator.hasNext()) {
				r2 = iterator.next();
				aftersort++;
				if (r1.getReadName().equals(r2.getReadName())) {
					aftersortMatch++;
					collate.writers[collate.getStreamIndex(r1)].addAlignment(r1);
					collate.writers[collate.getStreamIndex(r2)].addAlignment(r2);
					if (!iterator.hasNext())
						break;
					r1 = iterator.next();
					r2 = null;
				} else {
					collate.writers[0].addAlignment(r1);
					r1 = r2;
					r2 = null;
				}
			}
			if (r1 != null) {
				aftersort++;
				collate.writers[0].addAlignment(r1);
			}
			reader.close();

			System.out.println("Aftersort: " + aftersort);
			System.out.println("Aftersort matches: " + aftersortMatch);

			for (SAMFileWriter writer : collate.writers)
				writer.close();
		}

		if (params.cleanup) {
			overspillFile.delete();
			sorted.delete();
		}
	}

	@Parameters(commandDescription = "Split reads.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class)
		File file;

		@Parameter(names = { "--max-records" }, description = "Stop after reading this many records.")
		long maxRecords = -1;

		@Parameter(names = { "--compression" }, description = "BAM compression level. ")
		int compression = 5;

		@Parameter(names = { "--cleanup" }, description = "Remove tmp files.")
		boolean cleanup = false;
	}
}
