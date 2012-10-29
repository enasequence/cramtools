package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;

public class ExternalByteArrayEncoding implements Encoding<byte[]> {
	public static final EncodingID encodingId = EncodingID.EXTERNAL;
	public int contentId = -1;

	public ExternalByteArrayEncoding() {
	}

	public static EncodingParams toParam(int contentId) {
		ExternalByteArrayEncoding e = new ExternalByteArrayEncoding();
		e.contentId = contentId;
		return new EncodingParams(encodingId, e.toByteArray());
	}

	public byte[] toByteArray() {
		return ByteBufferUtils.writeUnsignedITF8(contentId);
	}

	public void fromByteArray(byte[] data) {
		contentId = ByteBufferUtils.readUnsignedITF8(data);
	}

	@Override
	public BitCodec<byte[]> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		InputStream is = inputMap == null ? null : inputMap.get(contentId);
		ExposedByteArrayOutputStream os = outputMap == null ? null : outputMap
				.get(contentId);
		return (BitCodec) new ExternalByteArrayCodec(os, is);
	}

	@Override
	public EncodingID id() {
		return encodingId;
	}

}
