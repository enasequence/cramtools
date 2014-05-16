package net.sf.cram.cg;

import java.nio.ByteBuffer;

class CigEl {
	int len;
	char op;
	ByteBuffer bases, scores;

	CigEl() {
	}

	CigEl(int len, char op) {
		this.len = len;
		this.op = op;
	}

	@Override
	public String toString() {
		if (bases != null) {
			bases.rewind();
			byte[] bytes = new byte[bases.limit()];
			bases.get(bytes);
			return String.format("%c, %d, %s", op, len, new String(bytes));
		} else
			return String.format("%c, %d, no bases", op, len);
	}
}