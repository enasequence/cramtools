package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.encoding.rans2.Encoding.RansEncSymbol;

public class Codec {
	public static final int ORDER_BYTE_LENGTH = 1;
	public static final int COMPRESSED_BYTE_LENGTH = 4;
	public static final int RAW_BYTE_LENGTH = 4;
	public static final int PREFIX_BYTE_LENGTH = ORDER_BYTE_LENGTH
			+ COMPRESSED_BYTE_LENGTH + RAW_BYTE_LENGTH;

	public static ByteBuffer uncompress(ByteBuffer in) {
		if (in.remaining() == 0)
			return ByteBuffer.allocate(0);

		int order = in.get();

		switch (order) {
		case 0:
			return D04.decode(in);

		case 1:
			return D14.decode(in.slice(), null);

		default:
			throw new RuntimeException("Unknown rANS order: " + order);
		}
	}

	public static ByteBuffer compress(ByteBuffer in, int order) {
		if (in.remaining() == 0)
			return ByteBuffer.allocate(0);

		if (in.remaining() < 4)
			return encode_order0_way4(in, null);

		switch (order) {
		case 0:
			return encode_order0_way4(in, null);
		case 1:
			return encode_order1_way4(in, null);

		default:
			throw new RuntimeException("Unknown rANS order: " + order);
		}
	}

	// private static ByteBuffer compress(ByteBuffer in, int order, int
	// parallel) {
	// switch (order) {
	// case 0:
	// switch (parallel) {
	// case 1:
	// return E01.encode(in, null);
	// case 4:
	// return encode_order0_way4(in, null);
	//
	// default:
	// throw new RuntimeException(
	// "Unknown compression request: parallel=" + parallel);
	// }
	// case 1:
	// return E14.encode(in.slice(), null);
	//
	// default:
	// throw new RuntimeException("Unknown rANS order: " + order);
	// }
	// }

	private static final ByteBuffer allocateIfNeeded(int in_size,
			ByteBuffer out_buf) {
		int compressedSize = (int) (1.05 * in_size + 257 * 257 * 3 + 4);
		if (out_buf == null)
			return ByteBuffer.allocate(compressedSize);
		if (out_buf.remaining() < compressedSize)
			throw new RuntimeException("Insuffient buffer size.");
		out_buf.order(ByteOrder.LITTLE_ENDIAN);
		return out_buf;
	}

	private static final ByteBuffer encode_order0_way4(ByteBuffer in,
			ByteBuffer out_buf) {
		int in_size = in.remaining();
		out_buf = allocateIfNeeded(in_size, out_buf);
		int freqTableStart = PREFIX_BYTE_LENGTH;
		out_buf.position(freqTableStart);

		int[] F = Freqs.calcFreqs_o0(in);
		RansEncSymbol[] syms = Freqs.buildSyms_o0(F);

		ByteBuffer cp = out_buf.slice();
		int frequencyTable_size = Freqs.writeFreqs_o0(cp, F);

		in.rewind();
		int compressedBlob_size = E04.compress(in, syms, cp);

		finilizeCompressed(0, out_buf, in_size, frequencyTable_size,
				compressedBlob_size);
		return out_buf;
	}

	private static final ByteBuffer encode_order1_way4(ByteBuffer in,
			ByteBuffer out_buf) {
		int in_size = in.remaining();
		out_buf = allocateIfNeeded(in_size, out_buf);
		int freqTableStart = PREFIX_BYTE_LENGTH;
		out_buf.position(freqTableStart);

		int[][] F = Freqs.calcFreqs_o1(in);
		RansEncSymbol[][] syms = Freqs.buildSyms_o1(F);

		ByteBuffer cp = out_buf.slice();
		int frequencyTable_size = Freqs.writeFreqs_o1(cp, F);

		in.rewind();
		int compressedBlob_size = E14.compress(in, syms, cp);

		finilizeCompressed(1, out_buf, in_size, frequencyTable_size,
				compressedBlob_size);
		return out_buf;
	}

	private static final void finilizeCompressed(int order, ByteBuffer out_buf,
			int in_size, int frequencyTable_size, int compressedBlob_size) {
		out_buf.limit(PREFIX_BYTE_LENGTH + frequencyTable_size
				+ compressedBlob_size);
		out_buf.put(0, (byte) order);
		out_buf.order(ByteOrder.LITTLE_ENDIAN);
		int compressedSizeOffset = ORDER_BYTE_LENGTH;
		out_buf.putInt(compressedSizeOffset, out_buf.limit()
				- ORDER_BYTE_LENGTH + COMPRESSED_BYTE_LENGTH);
		int rawSizeOffset = ORDER_BYTE_LENGTH + COMPRESSED_BYTE_LENGTH;
		out_buf.putInt(rawSizeOffset, in_size);
		out_buf.rewind();
	}
}
