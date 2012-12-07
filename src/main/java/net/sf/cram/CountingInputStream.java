package net.sf.cram;

import java.io.IOException;
import java.io.InputStream;

public class CountingInputStream extends InputStream {
	private InputStream delegate;
	private long count = 0;
	private boolean debug = false;

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public CountingInputStream(InputStream inputStream) {
		delegate = inputStream;
	}

	@Override
	public int read() throws IOException {
		count++;
		int read = delegate.read();
		if (debug)
			System.out.printf("pos=%d\tread=%d\n", count, read);
		return read;
	}

	public int read(byte[] b) throws IOException {
		int read = delegate.read(b);
		count += read;
		return read;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		int read = delegate.read(b, off, len);
		count += read;
		return read;
	}

	public long skip(long n) throws IOException {
		long skipped = delegate.skip(n);
		count += skipped;
		return skipped;
	}

	public int available() throws IOException {
		return delegate.available();
	}

	public void close() throws IOException {
		delegate.close();
	}

	public void mark(int readlimit) {
		delegate.mark(readlimit);
	}

	public void reset() throws IOException {
		delegate.reset();
		count = 0;
	}

	public boolean markSupported() {
		return delegate.markSupported();
	}

	public long getCount() {
		return count;
	}
}
