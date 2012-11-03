package net.sf.cram;

import java.util.List;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMReadGroupRecord;

public class CramNormalizer {
	private SAMFileHeader header ;
	private long readCounter ;
	private String readNamePrefix ;
	private int alignmentStart ;
	private byte defaultQualityScore;
	private SAMReadGroupRecord defaultReadGroup ;
	
	
	public void restoreReferenceSequence(int sequenceId, String sequenceName, List<CramRecord> records) {

	}
	
	public void restoreAlignmentStarts(int alignmentStart, List<CramRecord> records) {

	}

	public void restoreReadNames(long offset, String prefix, List<CramRecord> records) {

	}

	public void restoreReadBases(List<CramRecord> records) {

	}

	public void restoreQualityScores(byte defaultQualityScore, List<CramRecord> records) {

	}

	public void restoreMateInfo(SAMFileHeader header, List<CramRecord> records) {

	}
	
	public void restoreReadGroups(SAMReadGroupRecord defaultReadGroup, SAMFileHeader header, List<CramRecord> records) {

	}
}
