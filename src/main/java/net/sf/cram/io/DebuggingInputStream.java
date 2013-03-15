package net.sf.cram.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class DebuggingInputStream extends InputStream{
	private InputStream delegate ;

	public DebuggingInputStream(InputStream delegate) {
		super();
		this.delegate = delegate;
	}

	public int read() throws IOException {
		int value = delegate.read();
		System.err.println(value);
		return value ;
	}

	public int read(byte[] b) throws IOException {
		int value = delegate.read(b);
		System.err.println(Arrays.toString(b));
		return value ;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		int value = delegate.read(b, off, len);
		System.err.println(Arrays.toString(Arrays.copyOfRange(b, off, off+len)));
		return value ;
	}

	public long skip(long n) throws IOException {
		long value = delegate.skip(n);
		System.err.println("Skipping: " + n + ", " + value);
		return value ;
	}

	public int available() throws IOException {
		int value = delegate.available();
		System.err.println("Availble: " + value);
		return value ;
	}

	public void close() throws IOException {
		System.err.println("Close");
		delegate.close();
	}

	public void mark(int readlimit) {
		System.err.println("Mark: " + readlimit);
		delegate.mark(readlimit);
	}

	public void reset() throws IOException {
		System.err.println("Reset");
		delegate.reset();
	}

	public boolean markSupported() {
		boolean value = delegate.markSupported();
		System.err.println("Mark supported: " + value);
		return value ;
	}

}
