package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;


import net.sf.cram.EncodingID;
import net.sf.cram.io.ExposedByteArrayOutputStream;

public interface Encoding<T> {
	
	public EncodingID id() ;
	
	public byte[] toByteArray();

	public void fromByteArray(byte[] data);

	public BitCodec<T> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap);

}
