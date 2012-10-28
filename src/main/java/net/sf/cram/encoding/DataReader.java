package net.sf.cram.encoding;

public interface DataReader<T> {

	public T readData() ;
	public T readDataArray(int len) ;
}
