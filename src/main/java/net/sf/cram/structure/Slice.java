package net.sf.cram.structure;

import java.util.Map;


public class Slice {
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

	// content associated with ids:
	public Block headerBlock;
	public BlockContentType contentType;
	public Block coreBlock;
	public Block embeddedRefBlock;
	public Map<Integer, Block> external;
	
	// for indexing purposes:
	public int offset = -1 ;
	public long containerOffset = -1 ;
	public int size = -1;
	public int index = -1;
}