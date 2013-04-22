package net.sf.cram.encoding;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import net.sf.cram.encoding.huffint.CanonicalHuffmanByteCodec2;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.EncodingID;
import net.sf.cram.structure.EncodingParams;

public class HuffmanByteEncoding implements Encoding<Byte> {
	public static final EncodingID ENCODING_ID = EncodingID.HUFFMAN;
	private int[] bitLengths;
	private byte[] values;

	public HuffmanByteEncoding() {
	}

	@Override
	public EncodingID id() {
		return ENCODING_ID;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		ByteBufferUtils.writeUnsignedITF8(values.length, buf);
		for (byte value : values)
			buf.put(value);

		ByteBufferUtils.writeUnsignedITF8(bitLengths.length, buf);
		for (int value : bitLengths)
			ByteBufferUtils.writeUnsignedITF8(value, buf);

		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		return array;
	}

	@Override
	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);
		int size = ByteBufferUtils.readUnsignedITF8(buf);
		values = new byte[size];
		buf.get(values);

		size = ByteBufferUtils.readUnsignedITF8(buf);
		bitLengths = new int[size];
		for (int i = 0; i < size; i++)
			bitLengths[i] = ByteBufferUtils.readUnsignedITF8(buf);
	}

	@Override
	public BitCodec<Byte> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new CanonicalHuffmanByteCodec2(values, bitLengths);
	}

	public static EncodingParams toParam(byte[] bfValues, int[] bfBitLens) {
		HuffmanByteEncoding e = new HuffmanByteEncoding();
		e.values = bfValues;
		e.bitLengths = bfBitLens;
		return new EncodingParams(ENCODING_ID, e.toByteArray());
	}

}
