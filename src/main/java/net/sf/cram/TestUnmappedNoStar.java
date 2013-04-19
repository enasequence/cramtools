package net.sf.cram;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.CloseableIterator;

public class TestUnmappedNoStar {

	/**
	 * Expects the following args: path-to-ref-file path-to-cram-file [sequence
	 * name]
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Log.setGlobalLogLevel(LogLevel.INFO);

		Queue<String> q = new LinkedList<String>(Arrays.asList(args));

		File refFile = new File(q.poll());
		File cramFile = new File(q.poll());
		String query = q.poll();
		AlignmentSliceQuery asq = null;
		if (query != null)
			asq = new AlignmentSliceQuery(query);

		File indexFile = query == null ? null : new File(
				cramFile.getAbsolutePath() + ".bai");

		SAMFileReader
				.setDefaultValidationStringency(ValidationStringency.STRICT);
		System.setProperty("reference", refFile.getAbsolutePath());
		SAMFileReader reader = new SAMFileReader(cramFile, indexFile);

		CloseableIterator<SAMRecord> iterator;
		if (asq == null)
			iterator = reader.iterator();
		else
			iterator = reader.query(asq.sequence, asq.start, asq.end, true);

		int counter = 0;
		while (iterator.hasNext()) {
			counter++;
			SAMRecord record = iterator.next();
			if (!record.getReferenceName().equals(asq.sequence)) {
				System.err.println("Wrong sequence found:");
				System.err.println(record.getSAMString());
				System.out.println("Problem found. Examined " + counter
						+ " records. ");
				return;
			}

			boolean noStart_realRef = record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START
					&& !SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(record
							.getReferenceName());
			boolean realStart_noRef = record.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START
					&& SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(record
							.getReferenceName());

			if (noStart_realRef || realStart_noRef) {
				System.err.println("Found invalid record:");
				System.err.println(record.getSAMString());
				System.out.println("Problem found. Examined " + counter
						+ " records. ");
				return;
			}
		}

		System.out.println("Problem not found. Examined " + counter
				+ " records. ");
	}
}
