package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import uk.ac.ebi.ena.sra.cram.io.ByteBufferUtils;
import uk.ac.ebi.ena.sra.cram.io.ExposedByteArrayOutputStream;

import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class ExternalIntegerEncoding implements Encoding<Integer> {
	public static final EncodingID encodingId = EncodingID.EXTERNAL ;
	public int contentId = -1 ;

	public ExternalIntegerEncoding() {
	}
	
	public static EncodingParams toParam(int contentId) {
		ExternalIntegerEncoding e = new ExternalIntegerEncoding() ;
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
	public BitCodec<Integer> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		InputStream is = inputMap == null ? null : inputMap.get(contentId) ;
		ExposedByteArrayOutputStream os = outputMap == null ? null : outputMap.get(contentId) ;
		return (BitCodec) new ExternalIntegerCodec(os, is);
	}

	@Override
	public EncodingID id() {
		return encodingId;
	}

}
