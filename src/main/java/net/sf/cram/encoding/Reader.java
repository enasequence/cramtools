package net.sf.cram.encoding;

import net.sf.samtools.cram.CRAMRecord;

public class Reader {
	public DataReader<Integer> bitFlagsC;
	public DataReader<byte[]> readNameC;

	public void readInto(CRAMRecord r) {
		r.setFlags(bitFlagsC.readData());
		r.setReadName(new String(readNameC.readData()));
	}
}
