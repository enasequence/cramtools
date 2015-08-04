package net.sf.cram.cg;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTag;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Evidence2SAM {

	public static void main(String[] args) throws IOException, ParseException {
		EvidenceRecordFileIterator iterator = new EvidenceRecordFileIterator(new File(args[0]));
		Read context = new Read();
		SAMFileHeader header = new SAMFileHeader();
		header.setSortOrder(SAMFileHeader.SortOrder.unsorted);
		SAMSequenceRecord samSequenceRecord = new SAMSequenceRecord("chr10", 135534747);
		samSequenceRecord.setAttribute(SAMSequenceRecord.ASSEMBLY_TAG, iterator.assembly_ID);
		String readGroup = String.format("%s-%s", iterator.assembly_ID, iterator.chromosome);
		SAMReadGroupRecord readGroupRecord = new SAMReadGroupRecord(readGroup);
		readGroupRecord.setAttribute(SAMReadGroupRecord.READ_GROUP_SAMPLE_TAG, iterator.sample);
		readGroupRecord.setAttribute(SAMReadGroupRecord.PLATFORM_UNIT_TAG, readGroup);
		readGroupRecord.setAttribute(SAMReadGroupRecord.SEQUENCING_CENTER_TAG, "\"Complete Genomics\"");
		Date date = new SimpleDateFormat("yyyy-MMM-dd hh:mm:ss.S").parse(iterator.generatedAt);
		readGroupRecord.setAttribute(SAMReadGroupRecord.DATE_RUN_PRODUCED_TAG,
				new SimpleDateFormat("yyyy-MM-dd").format(date));
		readGroupRecord.setAttribute(SAMReadGroupRecord.PLATFORM_TAG, "\"Complete Genomics\"");
		header.addReadGroup(readGroupRecord);

		header.addSequence(samSequenceRecord);

		SAMFileWriterFactory f = new SAMFileWriterFactory();
		SAMFileWriter samWriter;
		if (args.length > 1)
			samWriter = f.makeBAMWriter(header, false, new File(args[1]));
		else
			samWriter = f.makeSAMWriter(header, false, System.out);
		int i = 0;
		long time = System.currentTimeMillis();
		DedupIterator dedupIt = new DedupIterator(iterator);

		while (dedupIt.hasNext()) {
			EvidenceRecord evidenceRecord = dedupIt.next();
			if (evidenceRecord == null)
				throw new RuntimeException();
			try {
				context.reset(evidenceRecord);
				context.parse();
			} catch (Exception e) {
				System.err.println("Failed on line:");
				System.err.println(evidenceRecord.line);
				throw new RuntimeException(e);
			}

			SAMRecord[] samRecords = context.toSAMRecord(header);
			for (SAMRecord samRecord : samRecords) {
				samRecord.setAttribute(SAMTag.RG.name(), readGroup);
				samWriter.addAlignment(samRecord);
			}

			i++;
			if (i % 1000 == 0) {
				if (System.currentTimeMillis() - time > 10 * 1000) {
					time = System.currentTimeMillis();
					System.err.println(i);
				}
			}

			if (i > 10000)
				break;
		}
		samWriter.close();
	}
}
