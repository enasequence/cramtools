package net.sf.cram.encoding;

import java.io.InputStream;
import java.util.Map;

import net.sf.block.ByteBufferUtils;
import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.EncodingID;

public class ExternalByteArrayEncoding implements Encoding<byte[]> {
	public final static EncodingID ID = EncodingID.EXTERNAL;
	private int contentId;

	public ExternalByteArrayEncoding() {
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

		return new ExternalByteArrayBitCodec(os, is, null);
	}
}
