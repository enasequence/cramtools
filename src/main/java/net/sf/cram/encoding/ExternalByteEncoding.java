package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class ExternalByteEncoding implements Encoding<Byte> {
	public static final EncodingID encodingId = EncodingID.EXTERNAL ;
	public int contentId = -1 ;

	public ExternalByteEncoding() {
	}
	
	public static EncodingParams toParam(int contentId) {
		ExternalByteEncoding e = new ExternalByteEncoding() ;
		e.contentId = contentId ;
		return new EncodingParams(encodingId, e.toByteArray()) ;
	}

	public byte[] toByteArray() {
		return ByteBufferUtils.writeUnsignedITF8(contentId) ;
	}

	public void fromByteArray(byte[] data) {
		contentId = ByteBufferUtils.readUnsignedITF8(data) ;
	}

	@Override
	public BitCodec<Byte> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		InputStream is = inputMap == null ? null : inputMap.get(contentId) ;
		ExposedByteArrayOutputStream os = outputMap == null ? null : outputMap.get(contentId) ;
		return (BitCodec) new ExternalByteCodec(os, is);
	}

}
