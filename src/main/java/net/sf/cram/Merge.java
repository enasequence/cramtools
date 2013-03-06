package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.index.BAMQueryFilteringIterator;
import net.sf.picard.io.IoUtil;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMIterator;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTextWriter;
import net.sf.samtools.util.CloseableIterator;
import net.sf.samtools.util.SeekableFileStream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Merge {

	public static void usage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Merge.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		Log.setGlobalLogLevel(params.logLevel);

		if (args.length == 0 || params.help) {
			usage(jc);
			System.exit(1);
		}

		if (params.reference == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		if (params.files == null || params.files.isEmpty()) {
			System.out.println("At least one CRAM or BAM file is required.");
			System.exit(1);
		}

		ReferenceSequenceFile refFile = null;
		if (params.reference != null) {
			System.setProperty("reference", params.reference.getAbsolutePath());
			refFile = ReferenceSequenceFileFactory
					.getReferenceSequenceFile(params.reference);
		} else {
			String prop = System.getProperty("reference");
			if (prop != null)
				refFile = ReferenceSequenceFileFactory
						.getReferenceSequenceFile(new File(prop));
		}

		AlignmentSliceQuery query = params.region == null ? null
				: new AlignmentSliceQuery(params.region);

		List<RecordSource> list = readFiles(params.files, refFile, query);

		StringBuffer mergeComment = new StringBuffer("Merged from:");
		for (RecordSource source : list) {
			mergeComment.append(" ").append(source.path);
		}

		resolveCollisions(list);
		SAMFileHeader header = mergeHeaders(list);
		header.addComment(mergeComment.toString());

		SAMFileWriter writer = null;
		if (params.outFile != null)
			if (!params.samFormat)
				writer = new SAMFileWriterFactory().makeBAMWriter(header, true,
						params.outFile);
			else
				writer = new SAMFileWriterFactory().makeSAMWriter(header, true,
						params.outFile);
		else if (!params.samFormat) {
			// hack to write BAM format to stdout:
			File file = File.createTempFile("bam", null);
			file.deleteOnExit();
			BAMFileWriter bamWriter = new BAMFileWriter(System.out, file);
			header.setSortOrder(SortOrder.coordinate);
			bamWriter.setHeader(header);
			writer = bamWriter;
		}

		else {
			writer = Utils.createSAMTextWriter(null, System.out, header, params.printSAMHeader) ;
		}

		MergedSAMRecordIterator mergedIterator = new MergedSAMRecordIterator(
				list, header);
		while (mergedIterator.hasNext()) {
			SAMRecord record = mergedIterator.next();
			// System.out.println("> "+record.getSAMString());
			writer.addAlignment(record);
		}

		mergedIterator.close();
		for (RecordSource source : list)
			source.close();

		// hack: BAMFileWriter may throw this when streaming to stdout, so
		// silently drop the exception if streaming out BAM format:
		try {
			writer.close();
		} catch (net.sf.samtools.util.RuntimeIOException e) {
			if (params.samFormat
					|| params.outFile != null
					|| !e.getMessage()
							.matches(
									"Terminator block not found after closing BGZF file.*"))
				throw e;
		}
	}

	private static List<RecordSource> readFiles(List<File> files,
			ReferenceSequenceFile refFile, AlignmentSliceQuery query)
			throws IOException {
		List<RecordSource> sources = new ArrayList<Merge.RecordSource>(
				files.size());

		for (File file : files) {
			IoUtil.assertFileIsReadable(file);

			RecordSource source = new RecordSource();
			sources.add(source);
			source.id = file.getName();
			source.path = file.getAbsolutePath();

			File index = new File(file.getAbsolutePath() + ".bai");
			if (index.exists()) {
				SAMFileReader reader = new SAMFileReader(file, index);
				source.reader = reader;
				if (query == null)
					source.it = reader.iterator();
				else
					source.it = reader.query(query.sequence, query.start,
							query.end, true);
			} else {
				index = new File(file.getAbsolutePath() + ".crai");
				if (index.exists()) {
					SAMFileReader reader = new SAMFileReader(file);
					source.reader = reader;
					if (query == null)
						source.it = reader.iterator();
					else {
						SeekableFileStream is = new SeekableFileStream(file);

						FileInputStream fis = new FileInputStream(index);
						GZIPInputStream gis = new GZIPInputStream(
								new BufferedInputStream(fis));
						BufferedInputStream bis = new BufferedInputStream(gis);
						List<Index.Entry> full = Index.readIndex(gis);

						List<Index.Entry> entries = new LinkedList<Index.Entry>();
						SAMSequenceRecord sequence = reader.getFileHeader()
								.getSequence(query.sequence);
						if (sequence == null)
							throw new RuntimeException("Sequence not found: "
									+ query.sequence);

						entries.addAll(Index.find(full,
								sequence.getSequenceIndex(), query.start,
								query.end - query.start));

						bis.close();

						SAMIterator it = new SAMIterator(is, refFile);
						is.seek(entries.get(0).offset);
						BAMQueryFilteringIterator bit = new BAMQueryFilteringIterator(
								it, query.sequence, query.start, query.end,
								BAMQueryFilteringIterator.QueryType.CONTAINED,
								reader.getFileHeader());
						source.it = bit;
					}
				} else {
					SAMFileReader reader = new SAMFileReader(file);
					source.reader = reader;
					source.it = reader.iterator();
				}
			}
		}

		return sources;
	}

	private static List<String> resolveCollisions(List<RecordSource> list) {

		ArrayList<String> result = new ArrayList<String>(list.size());

		// count list entries:
		Map<String, Integer> idCountMap = new TreeMap<String, Integer>();
		for (RecordSource source : list) {
			if (idCountMap.containsKey(source.id)) {
				idCountMap.put(source.id,
						((Integer) idCountMap.get(source.id)).intValue() + 1);
			} else
				idCountMap.put(source.id, 1);
		}

		// update entries with their number of occurrence except for singletons:
		for (int i = list.size() - 1; i >= 0; i--) {
			RecordSource source = list.get(i);
			int count = idCountMap.get(source.id);
			if (count > 1) {
				list.get(i).id = source.id + String.valueOf(count);
				idCountMap.put(source.id, --count);
			}
		}

		return result;
	}

	private static class RecordSource {
		private String path;
		private String id;
		private CloseableIterator<SAMRecord> it;
		private SAMFileReader reader;

		public RecordSource() {
		}

		public RecordSource(String id, SAMRecordIterator it) {
			this.id = id;
			this.it = it;
		}

		public void close() {
			if (it != null)
				it.close();
			if (reader != null)
				reader.close();
		}

	}

	private static class MergedSAMRecordIterator implements SAMRecordIterator {
		private char delim = '.';
		private List<RecordSource> sources;
		private SAMRecord nextRecord;
		private SAMFileHeader header;
		private PriorityQueue<SAMRecord> queue = new PriorityQueue<SAMRecord>(
				10000, new Comparator<SAMRecord>() {

					@Override
					public int compare(SAMRecord o1, SAMRecord o2) {
						int result = o1.getAlignmentStart()
								- o2.getAlignmentStart();
						if (result != 0)
							return result;
						else
							return o1.getReadName().compareTo(o2.getReadName());
					}

				});

		public MergedSAMRecordIterator(List<RecordSource> sources,
				SAMFileHeader header) {
			this.sources = sources;
			this.header = header;
			nextRecord = doNext();
		}

		@Override
		public void close() {
			for (RecordSource source : sources)
				source.it.close();
		}

		@Override
		public boolean hasNext() {
			return nextRecord != null;
		}

		private boolean milk() {
			boolean hasMore = false;
			int counter = 0;
			for (RecordSource source : sources) {
				counter++;
				CloseableIterator<SAMRecord> it = source.it;
				if (it.hasNext()) {
					SAMRecord record;
					try {
						record = (SAMRecord) it.next().clone();
						record.setReadName(source.id + delim
								+ record.getReadName());
						queue.add(record);
					} catch (CloneNotSupportedException e) {
						throw new RuntimeException(e);
					}
					hasMore = true;
				}
			}
			return hasMore;
		}

		private SAMRecord doNext() {
			SAMRecord nextRecord = null;
			do {
				nextRecord = queue.poll();
			} while (nextRecord == null && milk());

			if (nextRecord != null) {

				SAMSequenceRecord sequence = header.getSequence(nextRecord
						.getReferenceName());

				nextRecord.setHeader(header);

				nextRecord.setReferenceIndex(sequence.getSequenceIndex());
				if (nextRecord.getMateReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					nextRecord
							.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
				} else {
					SAMSequenceRecord mateSequence = header
							.getSequence(nextRecord.getMateReferenceName());
					nextRecord.setMateReferenceIndex(mateSequence
							.getSequenceIndex());
				}
			}

			return nextRecord;
		}

		@Override
		public SAMRecord next() {
			if (nextRecord == null)
				throw new RuntimeException("Iterator exchausted.");

			SAMRecord toReturn = nextRecord;
			nextRecord = doNext();

			return toReturn;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public SAMRecordIterator assertSorted(SortOrder sortOrder) {
			if (sortOrder != SortOrder.coordinate)
				throw new RuntimeException(
						"Only coordinate sort order is supported: "
								+ sortOrder.name());

			return null;
		}

	}

	private static SAMFileHeader mergeHeaders(List<RecordSource> sources) {
		SAMFileHeader header = new SAMFileHeader();
		for (RecordSource source : sources) {
			SAMFileHeader h = source.reader.getFileHeader();

			for (SAMSequenceRecord seq : h.getSequenceDictionary()
					.getSequences()) {
				if (header.getSequenceDictionary().getSequence(
						seq.getSequenceName()) == null)
					header.addSequence(seq);
			}

			for (SAMProgramRecord pro : h.getProgramRecords()) {
				if (header.getProgramRecord(pro.getProgramGroupId()) == null)
					header.addProgramRecord(pro);
			}

			for (String comment : h.getComments())
				header.addComment(comment);

			for (SAMReadGroupRecord rg : h.getReadGroups()) {
				if (header.getReadGroup(rg.getReadGroupId()) == null)
					header.addReadGroup(rg);
			}

		}
		return header;
	}

	@Parameters(commandDescription = "Tool to merge CRAM or BAM files. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "--output-file" }, converter = FileConverter.class, description = "Path to the output BAM file. Omit for stdout.")
		File outFile;

		@Parameter(names = { "--sam-format" }, description = "Output in SAM rather than BAM format.")
		boolean samFormat = false;

		@Parameter(names = { "--sam-header" }, description = "Print SAM file header when output format is text SAM.")
		boolean printSAMHeader = false;

		@Parameter(names = { "--region", "-r" }, description = "Alignment slice specification, for example: chr1:65000-100000.")
		String region;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(converter = FileConverter.class, description = "The paths to the CRAM or BAM files to uncompress. ")
		List<File> files;

	}

}
