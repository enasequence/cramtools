package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.encoding.rans2.Encoding.RansEncSymbol;
import net.sf.cram.io.ByteBufferUtils;

class E04 {
	static ByteBuffer encode(ByteBuffer in, ByteBuffer out_buf) {
		int in_size = in.remaining();
		if (out_buf == null)
			out_buf = ByteBuffer
					.allocate((int) (1.05 * in_size + 257 * 257 * 3 + 4));
		out_buf.position(1 + 4 + 4);

		RansEncSymbol[] syms = new RansEncSymbol[256];
		for (int i = 0; i < syms.length; i++)
			syms[i] = new RansEncSymbol();
		int rans0, rans1, rans2, rans3;
		int T = 0, i, j, tab_size;
		int[] C = new int[256];

		// Compute statistics
		int[] F = new int[256];
		for (i = 0; i < in_size; i++) {
			F[0xFF & in.get()]++;
			T++;
		}
		long tr = ((long) Constants.TOTFREQ << 31) / T + (1 << 30) / T;
		// // System.out.printf("tr=%d\n", tr);

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
		// // System.out.printf("fsum=%d\n", fsum);

		fsum++;
		if (fsum < Constants.TOTFREQ)
			F[M] += Constants.TOTFREQ - fsum;
		else
			F[M] -= fsum - Constants.TOTFREQ;

		// // System.err.printf("F[%d]=%d\n", M, F[M]);
		assert (F[M] > 0);

		// Encode statistics.
		// Not the most optimal encoding method, but simple
		ByteBuffer cp = out_buf.slice();

		int rle;
		for (T = rle = j = 0; j < 256; j++) {
			C[j] = T;
			T += F[j];
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
				Encoding.RansEncSymbolInit(syms[j], C[j], F[j],
						Constants.TF_SHIFT);
			}
		}

		cp.put((byte) 0);
		tab_size = cp.position();

		// output compressed bytes in FORWARD order:
		ByteBuffer ptr = cp.slice();

		rans0 = Constants.RANS_BYTE_L;
		rans1 = Constants.RANS_BYTE_L;
		rans2 = Constants.RANS_BYTE_L;
		rans3 = Constants.RANS_BYTE_L;

		switch (i = (in_size & 3)) {
		case 3:
			rans2 = Encoding.RansEncPutSymbol(rans2, ptr,
					syms[0xFF & in.get(in_size - (i - 2))]);
		case 2:
			rans1 = Encoding.RansEncPutSymbol(rans1, ptr,
					syms[0xFF & in.get(in_size - (i - 1))]);
		case 1:
			rans0 = Encoding.RansEncPutSymbol(rans0, ptr,
					syms[0xFF & in.get(in_size - (i - 0))]);
		case 0:
			break;
		}
		for (i = (in_size & ~3); i > 0; i -= 4) {
			int c3 = 0xFF & in.get(i - 1);
			int c2 = 0xFF & in.get(i - 2);
			int c1 = 0xFF & in.get(i - 3);
			int c0 = 0xFF & in.get(i - 4);

			rans3 = Encoding.RansEncPutSymbol(rans3, ptr, syms[c3]);
			rans2 = Encoding.RansEncPutSymbol(rans2, ptr, syms[c2]);
			rans1 = Encoding.RansEncPutSymbol(rans1, ptr, syms[c1]);
			rans0 = Encoding.RansEncPutSymbol(rans0, ptr, syms[c0]);
		}

		// // System.out.println("Flushing:");
		ptr.putInt(rans3);
		ptr.putInt(rans2);
		ptr.putInt(rans1);
		ptr.putInt(rans0);
		ptr.flip();
		int cdata_size = ptr.limit();
		// reverse the compressed bytes, so that they become in REVERSE order:
		ByteBufferUtils.reverse(ptr);

		// Finalise block size and return it
		out_buf.limit(1 + 4 + 4 + tab_size + cdata_size);
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
