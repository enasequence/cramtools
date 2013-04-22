package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;


import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.EncodingID;
import net.sf.cram.structure.EncodingParams;

public class GolombIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = EncodingID.GOLOMB;
	private int m;
	private int offset;

	public GolombIntegerEncoding() {
	}
	
	public GolombIntegerEncoding(int m) {
		this.m = m;
		this.offset = 0 ;
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int m) {
		GolombIntegerEncoding e = new GolombIntegerEncoding();
		e.m = m;
		e.offset = 0 ;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}
	
	public static EncodingParams toParam(int m, int offset) {
		GolombIntegerEncoding e = new GolombIntegerEncoding();
		e.m = m;
		e.offset = offset ;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.allocate(10);
		ByteBufferUtils.writeUnsignedITF8(offset, buf);
		ByteBufferUtils.writeUnsignedITF8(m, buf);
		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		return array;
	}

	@Override
	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data) ;
		offset = ByteBufferUtils.readUnsignedITF8(buf);
		m = ByteBufferUtils.readUnsignedITF8(buf);
	}

	@Override
	public BitCodec<Integer> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new GolombIntegerCodec(m, true, offset);
	}

}
