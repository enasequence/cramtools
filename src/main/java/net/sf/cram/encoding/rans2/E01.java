package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.encoding.rans2.Encoding.RansEncSymbol;
import net.sf.cram.io.ByteBufferUtils;

public class E01 {

	private static int[] calcFreqs(ByteBuffer in) {
		in = in.slice();
		int in_size = in.remaining();
		int T = 0, i, j;

		// Compute statistics
		int[] F = new int[256];
		for (i = 0; i < in_size; i++) {
			F[0xFF & in.get()]++;
			T++;
		}
		long tr = ((long) Constants.TOTFREQ << 31) / T + (1 << 30) / T;

		// Normalise so T[i] == TOTFREQ
		int m = 0, M = 0;
		for (j = 0; j < 256; j++) {
			if (m < F[j]) {
				m = F[j];
				M = j;
			}
		}

		int fsum = 0;
		for (j = 0; j < 256; j++) {
			if (F[j] == 0)
				continue;
			if ((F[j] = (int) ((F[j] * tr) >> 31)) == 0)
				F[j] = 1;
			fsum += F[j];
		}

		fsum++;
		if (fsum < Constants.TOTFREQ)
			F[M] += Constants.TOTFREQ - fsum;
		else
			F[M] -= fsum - Constants.TOTFREQ;

		assert (F[M] > 0);
		return F;
	}

	private static RansEncSymbol[] buildSyms(int[] F) {
		int T, j;
		int C[] = new int[256];
		RansEncSymbol[] syms = new RansEncSymbol[256];
		for (int i = 0; i < syms.length; i++)
			syms[i] = new RansEncSymbol();
		for (T = j = 0; j < 256; j++) {
			C[j] = T;
			T += F[j];
			if (F[j] != 0) {
				Encoding.RansEncSymbolInit(syms[j], C[j], F[j],
						Constants.TF_SHIFT);
			}
		}
		return syms;
	}

	private static void writeFreqs(ByteBuffer cp, int[] F) {
		int rle, j;
		for (rle = j = 0; j < 256; j++) {
			if (F[j] != 0) {
				// j
				if (rle != 0) {
					rle--;
				} else {
					cp.put((byte) j);
					if (rle == 0 && j != 0 && F[j - 1] != 0) {
						for (rle = j + 1; rle < 256 && F[rle] != 0; rle++)
							;
						rle -= j + 1;
						cp.put((byte) rle);
					}
				}

				// F[j]
				if (F[j] < 128) {
					cp.put((byte) (F[j]));
				} else {
					cp.put((byte) (128 | (F[j] >> 8)));
					cp.put((byte) (F[j] & 0xff));
				}
			}
		}

		cp.put((byte) 0);
	}

	private static void writeCompressedBlobTo(ByteBuffer data,
			RansEncSymbol[] syms, ByteBuffer out_buf) {
		int rans0 = Constants.RANS_BYTE_L;
		ByteBuffer cp = out_buf.slice();

		for (int i = data.remaining(); i > 0; i--) {
			int c0 = 0xFF & data.get(i - 1);
			rans0 = Encoding.RansEncPutSymbol(rans0, cp, syms[c0]);
		}

		cp.putInt(rans0);
		cp.flip();
		// reverse the compressed bytes, so that they become in REVERSE order:
		ByteBufferUtils.reverse(cp);
		out_buf.position(out_buf.position() + cp.limit());
	}

	public static ByteBuffer encodeBare(ByteBuffer in, ByteBuffer out_buf) {
		if (out_buf == null)
			throw new NullPointerException();

		// Compute statistics
		int[] F = calcFreqs(in);

		RansEncSymbol[] syms = buildSyms(F);
		// Encode statistics.
		// Not the most optimal encoding method, but simple
		writeFreqs(out_buf, F);

		writeCompressedBlobTo(in, syms, out_buf);
		return out_buf;
	}

	public static ByteBuffer encode(ByteBuffer in, ByteBuffer out_buf) {
		int in_size = in.remaining();
		if (out_buf == null)
			out_buf = ByteBuffer
					.allocate((int) (1.05 * in_size + 257 * 257 * 3 + 4));

		out_buf.position(1 + 4 + 4);
		encodeBare(in, out_buf);

		// Finalise block size and return it
		// out_buf.limit(1 + 4 + 4 + tab_size + cdata_size);
		out_buf.limit(out_buf.position());
		out_buf.put(0, (byte) 0);
		ByteOrder byteOrder = out_buf.order();
		out_buf.order(ByteOrder.LITTLE_ENDIAN);
		out_buf.putInt(1, out_buf.limit() - 5);
		out_buf.putInt(5, in_size);
		out_buf.order(byteOrder);
		out_buf.position(0);
		return out_buf;
	}
}
