package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;


import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;

@Deprecated
public class UnaryIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = null;
	private int offset;
	private boolean stopBit;

	public UnaryIntegerEncoding() {
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int offset, boolean stopBit) {
		UnaryIntegerEncoding e = new UnaryIntegerEncoding();
		e.offset = offset ;
		e.stopBit = stopBit ;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.allocate(10);
		ByteBufferUtils.writeUnsignedITF8(offset, buf);
		buf.put((byte) (stopBit ? 1 : 0));
		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		return array;
	}

	@Override
	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);
		offset = ByteBufferUtils.readUnsignedITF8(buf);
		stopBit = buf.get() == 1;
	}

	@Override
	public BitCodec<Integer> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new UnaryIntegerCodec();
	}

}
