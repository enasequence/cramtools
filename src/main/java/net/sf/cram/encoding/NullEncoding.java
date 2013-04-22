package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;


import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.EncodingID;
import net.sf.cram.structure.EncodingParams;

public class NullEncoding<T> implements Encoding<T> {
	public static final EncodingID ENCODING_ID = EncodingID.NULL;

	public NullEncoding() {
	}
	
	@Override
	public EncodingID id() {
		return ENCODING_ID;
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
