package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class GammaIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = EncodingID.BETA;

	public GammaIntegerEncoding() {
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int nofBits) {
		return new EncodingParams(ENCODING_ID, new byte[0]);
	}

	@Override
	public byte[] toByteArray() {
		return ByteBufferUtils.writeUnsignedITF8(nofBits);
	}

	@Override
	public void fromByteArray(byte[] data) {
		nofBits = ByteBufferUtils.readUnsignedITF8(data);
	}

	@Override
	public BitCodec<Integer> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new GammaIntegerCodec();
	}

}
