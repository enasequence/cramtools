package net.sf.cram;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.ref.ReferenceTracks;
import net.sf.cram.structure.CramRecord;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;

public class BenchBam2CramConvertMethod {

	public static void main(String[] args) throws FileNotFoundException {
		Log.setGlobalLogLevel(LogLevel.INFO) ;
		File bamFile;
		File refFile;

		if (args.length == 0) {
			File options = new File("options");
			Scanner scanner = new Scanner(options);
			bamFile = new File(scanner.nextLine());
			refFile = new File(scanner.nextLine());
			scanner.close();
		} else {
			bamFile = new File(args[0]);
			refFile = new File(args[1]);
		}

		SAMFileReader reader = new SAMFileReader(bamFile);
		SAMFileHeader samFileHeader = reader.getFileHeader();

		List<SAMRecord> samRecords = new ArrayList<SAMRecord>();
		Set<String> refsUsed = new TreeSet<String>();
		SAMRecordIterator iterator = reader.iterator();
		while (iterator.hasNext()) {
			SAMRecord samRecord = iterator.next();

			String refName = samRecord.getReferenceName();
			if (!refsUsed.isEmpty() && !refsUsed.contains(refName))
				break;

			refsUsed.add(refName);
			samRecords.add(samRecord);

			if (samRecords.size() >= 10000)
				break;
		}
		iterator.close();
		reader.close();

		String refName = refsUsed.iterator().next();
		SAMSequenceRecord sequence = samFileHeader.getSequence(refName);
		byte[] ref = new ReferenceSource(refFile).getReferenceBases(sequence,
				true);
		ReferenceTracks tracks = new ReferenceTracks(sequence.getSequenceIndex(), sequence.getSequenceName(), ref, ref.length) ;
		QualityScorePreservation preservation = new QualityScorePreservation(
				"*8");

		for (int i = 0; i < 1; i++) {
			List<CramRecord> cramRecords = Bam2Cram.convert(samRecords,
					samFileHeader, ref, tracks, preservation, false, null, null);
			System.out.println(cramRecords.get(0).toString());
		}
	}
}
