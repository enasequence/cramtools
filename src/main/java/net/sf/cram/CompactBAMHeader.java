package net.sf.cram;

import java.io.File;
import java.security.NoSuchAlgorithmException;

import net.sf.cram.common.Utils;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;

public class CompactBAMHeader {
	private static Log log = Log.getInstance(CompactBAMHeader.class);

	public static void main(String[] args) throws NoSuchAlgorithmException {
		File inFile = new File(args[0]);
		File outFile = new File(args[1]);
		File refFile = new File(args[2]);

		Log.setGlobalLogLevel(LogLevel.ERROR);

		SAMFileReader.setDefaultValidationStringency(ValidationStringency.SILENT);
		SAMFileReader reader = new SAMFileReader(inFile);
		long[] refids = new long[reader.getFileHeader().getSequenceDictionary().getSequences().size()];
		long unmapped = 0;
		long counter = 0;
		long time = System.currentTimeMillis();
		SAMRecordIterator it;
		for (it = reader.iterator(); it.hasNext();) {
			SAMRecord record = it.next();
			if (record.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
				unmapped++;
			else
				refids[record.getReferenceIndex()]++;

			if (record.getMateReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
				unmapped++;
			else
				refids[record.getMateReferenceIndex()]++;

			if (System.currentTimeMillis() - time > 1000 * 10) {
				time = System.currentTimeMillis();
				log.info("Read " + counter + " reads.");
			}
			counter++;
		}
		it.close();

		SAMSequenceDictionary d1 = reader.getFileHeader().getSequenceDictionary();
		SAMSequenceDictionary d2 = new SAMSequenceDictionary();
		for (int i = 0; i < refids.length; i++) {
			if (refids[i] > 0) {
				SAMSequenceRecord s = d1.getSequence(i);
				d2.addSequence(s.clone());
			}
		}

		log.info(String.format("Compacting %d\\%d.", d1.getSequences().size(), d2.getSequences().size()));

		SAMFileHeader h2 = reader.getFileHeader().clone();
		h2.setSequenceDictionary(d2);

		if (refFile != null) {
			ReferenceSequenceFile f = ReferenceSequenceFileFactory.getReferenceSequenceFile(refFile);
			for (SAMSequenceRecord s : h2.getSequenceDictionary().getSequences()) {
				String md5 = s.getAttribute(SAMSequenceRecord.MD5_TAG);
				// if (md5 != null)
				// continue;
				ReferenceSequence rs = f.getSequence(s.getSequenceName());
				if (rs == null) {
					log.error("Ref seq not found: " + s.getSequenceName());
					System.exit(1);
				}
				log.info("Loading ref seq " + rs.getName());
				byte[] bases = rs.getBases();
				Utils.upperCase(bases);
				log.info("Calculating md5 for " + rs.getName());
				md5 = Utils.calculateMD5String(bases);
				log.info(String.format("Sequence %s md5 is %s.", rs.getName(), md5));
				s.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
			}
		}

		SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(h2, h2.getSortOrder() == SortOrder.coordinate,
				outFile);

		counter = 0;
		time = System.currentTimeMillis();
		for (it = reader.iterator(); it.hasNext();) {
			SAMRecord record = it.next();
			record.setHeader(h2);
			if (record.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				int refid = h2.getSequenceIndex(record.getReferenceName());
				record.setReferenceIndex(refid);
			}
			if (record.getMateReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				int refid = h2.getSequenceIndex(record.getMateReferenceName());
				record.setMateReferenceIndex(refid);
			}
			writer.addAlignment(record);
			if (System.currentTimeMillis() - time > 1000 * 10) {
				time = System.currentTimeMillis();
				log.info("Written " + counter + " reads.");
			}
			counter++;
		}
		reader.close();
		writer.close();
	}
}
