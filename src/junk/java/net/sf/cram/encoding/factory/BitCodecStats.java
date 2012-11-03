package net.sf.cram.encoding.factory;

public class BitCodecStats {
	public String name;
	public long bitsConsumed, bitsProduced;
	public long arraysRead, arraysWritten;
	public long objectsRead, objectsWritten;

	public BitCodecStats(String name) {
		this.name = name;
	}
	
	public BitCodecStats() {
	}

	public void reset() {
		bitsConsumed = 0;
		bitsProduced = 0;
		arraysRead = 0;
		arraysWritten = 0;
		objectsRead = 0;
		objectsWritten = 0;
	}
}
