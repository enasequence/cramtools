package net.sf.cram.structure;

public class Container {
	// container header as defined in the specs:
	/**
	 * Byte size of the content excluding header.
	 */
	public int containerByteSize;
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;
	public int nofRecords = -1;
	public long globalRecordCounter = -1;

	public long bases = 0;
	public int blockCount = -1;
	public int[] landmarks;
	
	/**
	 * Container data
	 */
	public Block[] blocks ;

	public CompressionHeader h;

	// slices found in the container:
	public Slice[] slices;

	// for performance measurement:
	public long buildHeaderTime;
	public long buildSlicesTime;
	public long writeTime;
	public long parseTime;
	public long readTime;


	// for indexing:
	/**
	 * Container start in the stream.
	 */
	public long offset;

	@Override
	public String toString() {
		return String
				.format("seqid=%d, astart=%d, aspan=%d, records=%d, slices=%d, blocks=%d.",
						sequenceId, alignmentStart, alignmentSpan, nofRecords,
						slices == null ? -1 : slices.length, blockCount);
	}
}