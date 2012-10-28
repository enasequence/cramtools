package net.sf.cram.encoding;

import java.io.IOException;

public interface DataReader<T> {

	public T readData() throws IOException ;
	public T readDataArray(int len) ;
}
