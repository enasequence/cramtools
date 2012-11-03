package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import uk.ac.ebi.ena.sra.cram.io.ByteBufferUtils;
import uk.ac.ebi.ena.sra.cram.io.ExposedByteArrayOutputStream;

import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class GolombRiceIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = EncodingID.GOLOMB_RICE;
	private int offset;
	private int m;

	public GolombRiceIntegerEncoding() {
	}
	
	public GolombRiceIntegerEncoding(int m) {
		this.m = m;
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int offset, int m) {
		GolombRiceIntegerEncoding e = new GolombRiceIntegerEncoding();
		e.offset= offset;
		e.m = m;
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
		return new GolombRiceIntegerCodec(offset, m, true);
	}

}
