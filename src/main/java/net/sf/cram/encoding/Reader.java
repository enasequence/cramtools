package net.sf.cram.encoding;

import net.sf.cram.CramRecord;

public class Reader {
	public DataReader<Integer> bitFlagsC;
	public DataReader<byte[]> readNameC;

	public void readInto(CramRecord r) {
		r.setFlags(bitFlagsC.readData());
		r.setReadName(new String(readNameC.readData()));
	}
}
