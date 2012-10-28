package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class NullEncoding<T> implements Encoding<T> {
	public static final EncodingID ENCODING_ID = EncodingID.NULL;

	public NullEncoding() {
	}

	public static EncodingParams toParam() {
		return new EncodingParams(ENCODING_ID, new NullEncoding().toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		return new byte[] {};
	}

	@Override
	public void fromByteArray(byte[] data) {
	}

	@Override
	public BitCodec<T> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new NullCodec<T>();
	}

}
