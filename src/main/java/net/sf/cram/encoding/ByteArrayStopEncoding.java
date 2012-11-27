package net.sf.cram.encoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;

public class ByteArrayStopEncoding implements Encoding<byte[]> {
	public final static EncodingID ID = EncodingID.BYTE_ARRAY_STOP;
	private byte stopByte = 0;
	private int externalId;

	public ByteArrayStopEncoding() {
	}

	@Override
	public EncodingID id() {
		return ID;
	}

	public ByteArrayStopEncoding(byte stopByte, int externalId) {
		this.stopByte = stopByte;
		this.externalId = externalId;
	}

	public static EncodingParams toParam(byte stopByte, int externalId) {
		ByteArrayStopEncoding e = new ByteArrayStopEncoding(stopByte,
				externalId);
		EncodingParams params = new EncodingParams(ID, e.toByteArray());
		return params;
	}

	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.put(stopByte);
		buf.putInt(externalId);

		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);

		return array;
	}

	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		stopByte = buf.get();
		externalId = buf.getInt();
	}

	@Override
	public BitCodec<byte[]> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		InputStream is = inputMap == null ? null : inputMap.get(externalId);
		ExposedByteArrayOutputStream os = outputMap == null ? null : outputMap
				.get(externalId);
		return new ByteArrayStopCodec(stopByte, is, os);
	}

	public static class ByteArrayStopCodec implements BitCodec<byte[]> {

		private byte stop;
		private InputStream is;
		private OutputStream os;
		private ByteArrayOutputStream readingBAOS = new ByteArrayOutputStream();

		public ByteArrayStopCodec(byte stopByte, InputStream is, OutputStream os) {
			stop = stopByte;
			this.is = is;
			this.os = os;
		}

		@Override
		public byte[] read(BitInputStream bis) throws IOException {
			int b;
			readingBAOS.reset();
			while ((b = is.read()) != -1 && b != stop)
				readingBAOS.write(b);

			return readingBAOS.toByteArray();
		}

		@Override
		public byte[] read(BitInputStream bis, int len) throws IOException {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public long write(BitOutputStream bos, byte[] object)
				throws IOException {
			os.write(object);
			os.write(stop);
			return object.length + 1;
		}

		@Override
		public long numberOfBits(byte[] object) {
			return object.length + 1;
		}

	}
}
