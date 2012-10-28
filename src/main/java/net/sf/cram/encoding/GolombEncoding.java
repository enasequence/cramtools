package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class GolombEncoding implements Encoding<Long> {
	public static final EncodingID ENCODING_ID = EncodingID.GOLOMB;
	private int m;

	public GolombEncoding() {
	}

	public static EncodingParams toParam(int m) {
		GolombEncoding e = new GolombEncoding();
		e.m = m;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		return ByteBufferUtils.writeUnsignedITF8(m);
	}

	@Override
	public void fromByteArray(byte[] data) {
		m = ByteBufferUtils.readUnsignedITF8(data);
	}

	@Override
	public BitCodec<Long> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new GolombCodec(m);
	}

}
