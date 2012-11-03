package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.IOUtils;


public class ExternalByteArrayCodec implements BitCodec<byte[]> {
	private OutputStream os;
	private InputStream is;

	public ExternalByteArrayCodec(OutputStream os, InputStream is) {
		this.os = os;
		this.is = is;
	}

	@Override
	public byte[] read(BitInputStream bis, int len) throws IOException {
		return IOUtils.readFully(is, len);
	}

	@Override
	public long write(BitOutputStream bos, byte[] object) throws IOException {
		os.write(object);
		return numberOfBits(object);
	}

	@Override
	public long numberOfBits(byte[] object) {
		return object.length * 8;
	}

	@Override
	public byte[] read(BitInputStream bis) throws IOException {
		throw new RuntimeException("Cannot read byte array of unknown length.") ;
	}

}
