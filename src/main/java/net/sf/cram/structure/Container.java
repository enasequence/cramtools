package net.sf.cram.structure;



public class Container {
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;

	public int nofRecords = -1;

	public CompressionHeader h;

	public Slice[] slices;
	public int blockCount;
	public int[] landmarks ;
	
	public long bases ;
	public long buildHeaderTime ;
	public long buildSlicesTime ;
	public long writeTime ;
	
	public long parseTime ;
	public long readTime ;

	@Override
	public String toString() {
		return String
				.format("seqid=%d, astart=%d, aspan=%d, records=%d, slices=%d, blocks=%d.",
						sequenceId, alignmentStart, alignmentSpan,
						nofRecords, slices == null ? -1 : slices.length,
						blockCount);
	}
}