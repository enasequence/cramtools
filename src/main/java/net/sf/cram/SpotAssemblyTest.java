package net.sf.cram;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import uk.ac.ebi.ena.sra.cram.bam.Sam2CramRecordFactory;
import uk.ac.ebi.ena.sra.cram.format.CramRecord;
import uk.ac.ebi.ena.sra.cram.spot.PairedTemplateAssembler;

public class SpotAssemblyTest {
	private List<CramRecord> records;
	private long counter;

	public void count(List<CramRecord> records) {
		for (CramRecord record : records)
			record.counter = counter++;
	}

	public void connect(List<CramRecord> records) {
		List<CramRecord> sortedByName = new ArrayList<CramRecord>(records.size());
		sortedByName.addAll(records);
		Collections.sort(sortedByName, byNameComparator);

		int i = 0;
		CramRecord r1, r2;

		int connected = 0;

		for (i = 1; i < sortedByName.size(); i++) {
			r1 = sortedByName.get(i - 1);
			String name = r1.getReadName();

			int link = 0;
			while (i < sortedByName.size() && (r2 = sortedByName.get(i)).getReadName().equals(name)) {
				link++;
				r1.detached = false;
				r2.detached = false;

				r1.next = r2;
				r2.previous = r1;

				r1.setRecordsToNextFragment(r2.counter - r1.counter);
				r2.setRecordsToNextFragment(r1.counter - r2.counter);

				r1.setReadName(null);
				r2.setReadName(null);

				i++;
				connected++;
				r1 = r2;
			}

			// while (++i < sortedByName.size()) {
			// r1 = sortedByName.get(i - 1);
			// r2 = sortedByName.get(i);
			//
			// if (r1.getReadName().equals(r2.getReadName())) {
			// link++;
			// r1.detached = false;
			// r2.detached = false;
			//
			// r1.next = r2;
			// r2.previous = r1;
			//
			// r1.setRecordsToNextFragment(r2.counter - r1.counter);
			// r2.setRecordsToNextFragment(r1.counter - r2.counter);
			// connected++;
			// } else {
			// break;
			// }
			// }
		}

		System.out.println("Connected: " + connected);
	}

	public void restoreReadNames(List<CramRecord> records) {
		count(records);
		int index = 0;
		int restored = 0;
		for (CramRecord record : records) {
			if (!record.detached && record.getRecordsToNextFragment() > 0) {
				CramRecord mate = records.get((int) (record.getRecordsToNextFragment() + index));
				String readName = String.valueOf(index);
				record.setReadName(readName);
				mate.setReadName(readName);
				restored++;
			}
			index++;
		}

		System.out.println("Restored: " + restored);
	}

	private static Comparator<CramRecord> byNameComparator = new Comparator<CramRecord>() {

		@Override
		public int compare(CramRecord o1, CramRecord o2) {
			return o1.getReadName().compareTo(o2.getReadName());
		}
	};

	public static void main(String[] args) {
		File bamFile = new File(args[0]);
		SAMFileReader reader = new SAMFileReader(bamFile);

		ReferenceSequenceFile ref = ReferenceSequenceFileFactory.getReferenceSequenceFile(new File(args[1]));
		SAMRecordIterator iterator = reader.iterator();
		String refName = iterator.next().getReferenceName();
		iterator.close();
		ReferenceSequence sequence = ref.getSequence(refName);

		iterator = reader.iterator();
		Sam2CramRecordFactory factory = new Sam2CramRecordFactory(sequence.getBases());
		factory.preserveReadNames = false;
		factory.captureAllTags = false;
		factory.captureUnmappedBases = true;
		factory.captureUnmappedScores = true;
		factory.preserveReadNames = true;

		List<CramRecord> cramRecords = new ArrayList<CramRecord>();
		List<SAMRecord> samRecords = new ArrayList<SAMRecord>();
		for (int i = 0; i < 100000; i++) {
			SAMRecord samRecord = iterator.next();
			samRecords.add(samRecord);
			CramRecord cramRecord = factory.createCramRecord(samRecord);
			// cramRecord.setReadName(samRecord.getReadName()) ;
			cramRecord.counter = i;
			cramRecords.add(cramRecord);

			// a.addSAMRecord(samRecord) ;
		}
		reader.close();

		long time1 = System.nanoTime();
		new SpotAssemblyTest().connect(cramRecords);
		long time2 = System.nanoTime();

		long time3 = System.nanoTime();
		new SpotAssemblyTest().restoreReadNames(cramRecords);
		long time4 = System.nanoTime();

		System.out.printf("%.3f\t%.3f\n", (time2 - time1) / 1000000f, (time4 - time3) / 1000000f);

		PairedTemplateAssembler a = new PairedTemplateAssembler();
		for (SAMRecord record : samRecords) {
			a.addSAMRecord(record);
		}

		time1 = System.nanoTime();
		int next = 0, fetch = 0; long maxD= 0;
		for (int i = 0; i < samRecords.size(); i++) {
			SAMRecord record = a.nextSAMRecord();
			if (a == null) {
				record = a.fetchNextSAMRecord();
				fetch++;
			} else
				next++;
			
			long d = a.distanceToNextFragment() ;
			if (maxD < d) maxD = d ;
			
		}
		time2 = System.nanoTime();
		
		System.out.printf("%.3f, %d, %d, %d\n", (time2 - time1) / 1000000f, next, fetch, maxD);
	}
}
