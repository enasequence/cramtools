package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.ByteBufferUtils;


public class ExternalIntegerCodec extends AbstractBitCodec<Integer> {
	private OutputStream os;
	private InputStream is;
	private OutputStream nullOS = new OutputStream() {

		@Override
		public void write(byte[] b) throws IOException {
		}

		@Override
		public void write(int b) throws IOException {
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
		}
	};

	public ExternalIntegerCodec(OutputStream os, InputStream is) {
		this.os = os;
		this.is = is;
	}

	@Override
	public Integer read(BitInputStream bis) throws IOException {
		return ByteBufferUtils.readUnsignedITF8(is);
	}

	@Override
	public long write(BitOutputStream bos, Integer value) throws IOException {
		return ByteBufferUtils.writeUnsignedITF8(value, os);
	}

	@Override
	public long numberOfBits(Integer value) {
		try {
			return ByteBufferUtils.writeUnsignedITF8(value, nullOS);
		} catch (IOException e) {
			// this should never happened but still:
			throw new RuntimeException(e) ;
		}
	}

	@Override
	public Integer read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented.");
	}
}
