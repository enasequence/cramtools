package net.sf.cram.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;

public abstract class AbstractBitCodec<T> implements BitCodec<T> {

	@Override
	public abstract T read(BitInputStream bis) throws IOException;

	@Override
	public abstract T read(BitInputStream bis, int valueLen) throws IOException;

	@Override
	public void readInto(BitInputStream bis, byte[] array, int offset, int valueLen)
			throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public void readInto(BitInputStream bis, ByteBuffer buf, int valueLen)
			throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public void skip(BitInputStream bis) throws IOException {
		read(bis);
	}

	@Override
	public void skip(BitInputStream bis, int len) throws IOException {
		read(bis, len);
	}

	@Override
	public abstract long write(BitOutputStream bos, T object)
			throws IOException;

	@Override
	public abstract long numberOfBits(T object);

}
