package net.sf.cram.structure;

import java.util.Map;


public class Slice {
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;

	public int nofRecords = -1;

	public BlockContentType contentType;
	public Block coreBlock;
	public Map<Integer, Block> external;
}