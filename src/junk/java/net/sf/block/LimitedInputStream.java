package net.sf.block;

import java.io.IOException;
import java.io.InputStream;

class LimitedInputStream extends InputStream {
	private int limit = 0;
	private InputStream delegate;

	public LimitedInputStream(int limit, InputStream delegate) {
		super();
		this.limit = limit;
		this.delegate = delegate;
	}

	@Override
	public int read() throws IOException {
		if (limit < 1)
			return -1;
		limit--;
		return delegate.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (limit < 1)
			return -1;

		if (limit < len)
			len = (int) limit;

		int result = delegate.read(b, off, len);
		limit -= result;
		return result;
	}

	public void skipToEnd() throws IOException {
		if (limit > 0)
			delegate.skip(limit);
	}

}