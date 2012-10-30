package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class BetaIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID ENCODING_ID = EncodingID.BETA;
	private int nofBits;

	public BetaIntegerEncoding() {
	}

	public BetaIntegerEncoding(int nofBits) {
		this.nofBits = nofBits;
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	public static EncodingParams toParam(int nofBits) {
		BetaIntegerEncoding e = new BetaIntegerEncoding();
		e.nofBits = nofBits;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
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
		return new BetaIntegerCodec(0, nofBits);
	}

}
