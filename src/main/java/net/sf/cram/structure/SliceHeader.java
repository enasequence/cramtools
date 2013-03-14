package net.sf.cram.structure;

public class SliceHeader {
	// as defined in the specs:
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;
	public int nofRecords = -1;
	public long globalRecordCounter = -1;
	public int nofBlocks = -1;
	public int[] contentIDs ;
	public int embeddedRefBlockContentID ;
	public byte[] refMD5 ;
}
