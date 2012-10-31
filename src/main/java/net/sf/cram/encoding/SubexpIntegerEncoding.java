package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class SubexpIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = EncodingID.SUBEXP;
	private int offset;
	private int k;

	public SubexpIntegerEncoding() {
	}

	public SubexpIntegerEncoding(int k) {
		this(0, k);
	}

	public SubexpIntegerEncoding(int offset, int k) {
		this.offset = offset;
		this.k = k;
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int offset, int k) {
		SubexpIntegerEncoding e = new SubexpIntegerEncoding();
		e.offset = offset;
		e.k = k;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		return ByteBufferUtils.writeUnsignedITF8(k);
	}

	@Override
	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);
		offset = ByteBufferUtils.readUnsignedITF8(buf);
		k = ByteBufferUtils.readUnsignedITF8(buf);
	}

	@Override
	public BitCodec<Integer> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new SubexpIntegerCodec(offset, k);
	}

}
