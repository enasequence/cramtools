package net.sf.cram.fasta;

class FAIDX_FastaIndexEntry implements Comparable<FAIDX_FastaIndexEntry> {
	private String name;
	private int len;
	private long startPointer;
	private int lineWidthNoNL;
	private int lineWidthWithNL;

	public FAIDX_FastaIndexEntry(String name, int len, long startPointer, int lineWidthNoNL, int lineWidthWithNL) {
		this.name = name;
		this.len = len;
		this.startPointer = startPointer;
		this.lineWidthNoNL = lineWidthNoNL;
		this.lineWidthWithNL = lineWidthWithNL;
	}

	@Override
	public String toString() {
		return String.format("%s\t%d\t%d\t%d\t%d", name, len, startPointer, lineWidthNoNL, lineWidthWithNL);
	}

	@Override
	public int compareTo(FAIDX_FastaIndexEntry o) {
		int result = name.compareTo(o.name);
		if (result != 0)
			return result;

		return (int) (startPointer - o.startPointer);
	}

}