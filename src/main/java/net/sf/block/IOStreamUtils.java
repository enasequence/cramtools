package net.sf.block;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class IOStreamUtils {

	public final static byte[] readFully(InputStream is, int len)
			throws IOException {
		byte[] b = new byte[len];
		int off = 0;
		if (len < 0)
			throw new IndexOutOfBoundsException();
		int n = 0;
		while (n < len) {
			int count = is.read(b, off + n, len - n);
			if (count < 0)
				throw new EOFException();
			n += count;
		}

		return b;
	}
}
