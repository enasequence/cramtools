package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ExposedByteArrayOutputStream;

public interface Encoding<T> {
	
	public byte[] toByteArray();

	public void fromByteArray(byte[] data);

	public BitCodec<T> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap);

}
