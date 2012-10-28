package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.block.IOStreamUtils;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

public class ExternalByteArrayBitCodec implements BitCodec<byte[]> {
	private OutputStream os;
	private InputStream is;
	private String name;

	public ExternalByteArrayBitCodec(OutputStream os, InputStream is,
			String name) {
		this.os = os;
		this.is = is;
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public byte[] read(BitInputStream bis, int len) throws IOException {
		return IOStreamUtils.readFully(is, len);
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
