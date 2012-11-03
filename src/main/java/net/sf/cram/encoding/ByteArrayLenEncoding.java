package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingParams;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.ByteBufferUtils;
import uk.ac.ebi.ena.sra.cram.io.ExposedByteArrayOutputStream;

public class ByteArrayLenEncoding implements Encoding<byte[]> {
	public final static EncodingID ID = EncodingID.BYTE_ARRAY_LEN;
	private Encoding<Integer> lenEncoding;
	private Encoding<byte[]> byteEncoding;

	public ByteArrayLenEncoding() {
	}

	@Override
	public EncodingID id() {
		return ID;
	}

	public static EncodingParams toParam(EncodingParams lenParams,
			EncodingParams byteParams) {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		buf.put((byte) lenParams.id.ordinal());
		ByteBufferUtils.writeUnsignedITF8(lenParams.params.length, buf);
		buf.put(lenParams.params);

		buf.put((byte) byteParams.id.ordinal());
		ByteBufferUtils.writeUnsignedITF8(byteParams.params.length, buf);
		buf.put(byteParams.params);

		buf.flip();
		byte[] data = new byte[buf.limit()];
		buf.get(data);

		EncodingParams params = new EncodingParams(ID, data);
		return params;
	}

	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		buf.put((byte) lenEncoding.id().ordinal());
		byte[] lenBytes = lenEncoding.toByteArray();
		ByteBufferUtils.writeUnsignedITF8(lenBytes.length, buf);
		buf.put(lenBytes);

		buf.put((byte) byteEncoding.id().ordinal());
		byte[] byteBytes = lenEncoding.toByteArray();
		ByteBufferUtils.writeUnsignedITF8(byteBytes.length, buf);
		buf.put(byteBytes);

		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);

		return array;
	}

	public void fromByteArray(byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);

		EncodingFactory f = new EncodingFactory();

		EncodingID lenID = EncodingID.values()[buf.get()];
		lenEncoding = f.createEncoding(DataSeriesType.INT, lenID);
		int len = ByteBufferUtils.readUnsignedITF8(buf);
		byte[] bytes = new byte[len];
		buf.get(bytes);
		lenEncoding.fromByteArray(bytes);

		EncodingID byteID = EncodingID.values()[buf.get()];
		byteEncoding = f.createEncoding(DataSeriesType.BYTE_ARRAY, byteID);
		len = ByteBufferUtils.readUnsignedITF8(buf);
		bytes = new byte[len];
		buf.get(bytes);
		byteEncoding.fromByteArray(bytes);
	}

	@Override
	public BitCodec<byte[]> buildCodec(Map<Integer, InputStream> inputMap,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		return new ByteArrayLenCodec(
				lenEncoding.buildCodec(inputMap, outputMap),
				byteEncoding.buildCodec(inputMap, outputMap));
	}

	private static class ByteArrayLenCodec implements BitCodec<byte[]> {
		private BitCodec<Integer> lenCodec;
		private BitCodec<byte[]> byteCodec;

		public ByteArrayLenCodec(BitCodec<Integer> lenCodec,
				BitCodec<byte[]> byteCodec) {
			super();
			this.lenCodec = lenCodec;
			this.byteCodec = byteCodec;
		}

		@Override
		public byte[] read(BitInputStream bis) throws IOException {
			int len = lenCodec.read(bis);
			return byteCodec.read(bis, len);
		}

		@Override
		public byte[] read(BitInputStream bis, int len) throws IOException {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public long write(BitOutputStream bos, byte[] object)
				throws IOException {
			long len = lenCodec.write(bos, object.length);
			len += byteCodec.write(bos, object);
			return len;
		}

		@Override
		public long numberOfBits(byte[] object) {
			return lenCodec.numberOfBits(object.length)
					+ byteCodec.numberOfBits(object);
		}

	}
}
