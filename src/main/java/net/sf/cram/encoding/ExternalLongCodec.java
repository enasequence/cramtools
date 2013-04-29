package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;


public class ExternalLongCodec extends AbstractBitCodec<Long> {
	private OutputStream os;
	private InputStream is;

	public ExternalLongCodec(OutputStream os, InputStream is) {
		this.os = os;
		this.is = is;
	}

	@Override
	public Long read(BitInputStream bis) throws IOException {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= is.read();
		}
		return result;
	}

	@Override
	public long write(BitOutputStream bos, Long value) throws IOException {
		for (int i=0; i<8; i++) {
			os.write((int) (value & 0xFF)) ;
			value >>>= 8;
		}
		return 64;
	}

	@Override
	public long numberOfBits(Long object) {
		return 8;
	}

	@Override
	public Long read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented.");
	}
}
