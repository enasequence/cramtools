package net.sf.cram.encoding;

import java.io.IOException;

public interface DataWriter<T> {

	public long writeData (T value) throws IOException ;
}
