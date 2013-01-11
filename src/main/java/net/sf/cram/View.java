package net.sf.cram;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import net.sf.picard.io.IoUtil;
import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class View {

	public static void usage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ View.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

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

		AlignmentSliceQuery query = new AlignmentSliceQuery(params.region);

		List<SAMFileReader> readers = new ArrayList<SAMFileReader>(
				params.files.size());
		StringBuffer mergeComment = new StringBuffer("Merged from:");
		for (File file : params.files) {
			IoUtil.assertFileIsReadable(file);
			File index = new File(file.getAbsolutePath() + ".bai");
			if (!index.exists())
				index = new File(file.getAbsolutePath() + ".crai");
			if (!index.exists())
				index = null;

			if (params.reference != null)
				System.setProperty("reference",
						params.reference.getAbsolutePath());
			SAMFileReader reader = new SAMFileReader(file, index);

			// reader = new CRAMFileReader(new SingleSeekableStreamFactory(new
			// BufferedInputStream(new FileInputStream(
			// file))), new FileInputStreamFactory(file), params.reference,
			// index);

			readers.add(reader);

			mergeComment.append(" ").append(file.getAbsolutePath());
		}

		SAMFileHeader header = mergeHeaders(readers);
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
			BAMFileWriter bamWriter = new BAMFileWriter(
					new BufferedOutputStream(System.out), file);
			header.setSortOrder(SortOrder.coordinate);
			bamWriter.setHeader(header);
			writer = bamWriter;
		}

		else
			writer = new SAMFileWriterFactory().makeSAMWriter(header, true,
					new BufferedOutputStream(System.out));

		List<SAMRecordIterator> iterators = new ArrayList<SAMRecordIterator>(
				readers.size());
		for (SAMFileReader reader : readers) {
			SAMRecordIterator it = reader.query(query.sequence, query.start,
					query.end, false);
			iterators.add(it);
		}

		MergedSAMRecordIterator mergedIterator = new MergedSAMRecordIterator(
				iterators, header);
		while (mergedIterator.hasNext()) {
			writer.addAlignment(mergedIterator.next());
		}

		mergedIterator.close();
		for (SAMFileReader reader : readers)
			reader.close();

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

	private static class MergedSAMRecordIterator implements SAMRecordIterator {

		private List<SAMRecordIterator> iterators;
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

		public MergedSAMRecordIterator(List<SAMRecordIterator> iterators,
				SAMFileHeader header) {
			this.iterators = iterators;
			this.header = header;
			nextRecord = doNext();
		}

		@Override
		public void close() {
			for (SAMRecordIterator it : iterators)
				it.close();
		}

		@Override
		public boolean hasNext() {
			return nextRecord != null;
		}

		private boolean milk() {
			boolean hasMore = false;
			int counter = 0;
			for (SAMRecordIterator it : iterators) {
				counter++;
				if (it.hasNext()) {
					SAMRecord record;
					try {
						record = (SAMRecord) it.next().clone();
						record.setReadName(String.valueOf(counter) + "_"
								+ record.getReadName());
						queue.add(record);
					} catch (CloneNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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

	private static SAMFileHeader mergeHeaders(List<SAMFileReader> readers) {
		SAMFileHeader header = new SAMFileHeader();
		for (SAMFileReader reader : readers) {
			SAMFileHeader h = reader.getFileHeader();

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

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {

		@Parameter(names = { "--reference-fasta-file" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "--output-file" }, converter = FileConverter.class, description = "Path to the output BAM file. Omit for stdout.")
		File outFile;

		@Parameter(names = { "--sam-format" }, description = "Output in SAM rather than BAM format.")
		boolean samFormat = false;

		@Parameter(names = { "--region", "-r" }, description = "Alignment slice specification, for example: chr1:65000-100000.")
		String region;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(converter = FileConverter.class, description = "The paths to the CRAM or BAM files to uncompress. ")
		List<File> files;

	}

}
