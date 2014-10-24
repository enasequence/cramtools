package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.encoding.rans2.Encoding.RansEncSymbol;
import net.sf.cram.io.ByteBufferUtils;

class E14 {

	static ByteBuffer encode(ByteBuffer in, ByteBuffer out_buf) {
		int in_size = in.remaining();
		if (out_buf == null)
			out_buf = ByteBuffer
					.allocate((int) (1.05 * in_size + 257 * 257 * 3 + 4));
		out_buf.position(1 + 4 + 4);

		int tab_size;
		RansEncSymbol[][] syms = new RansEncSymbol[256][256];
		for (int i = 0; i < syms.length; i++)
			for (int j = 0; j < syms[i].length; j++)
				syms[i][j] = new RansEncSymbol();

		ByteBuffer cp = out_buf.slice();

		int[][] F = new int[256][256];
		int[] T = new int[256];
		int i, j, c;
		int rle_i, rle_j;

		for (int last_i = i = 0; i < in_size; i++) {
			F[last_i][c = (0xFF & in.get())]++;
			T[last_i]++;
			last_i = c;
		}
		F[0][in.get(1 * (in_size >> 2))]++;
		F[0][in.get(2 * (in_size >> 2))]++;
		F[0][in.get(3 * (in_size >> 2))]++;
		T[0] += 3;

		// Normalise so T[i] == TOTFREQ
		for (rle_i = i = 0; i < 256; i++) {
			int t2, m, M;
			int x;

			if (T[i] == 0)
				continue;

			// uint64_t p = (TOTFREQ * TOTFREQ) / t;
			double p = ((double) Constants.TOTFREQ) / T[i];
			for (t2 = m = M = j = 0; j < 256; j++) {
				if (F[i][j] == 0)
					continue;

				if (m < F[i][j]) {
					m = F[i][j];
					M = j;
				}

				// if ((F[i][j] = (F[i][j] * p) / TOTFREQ) == 0)
				if ((F[i][j] *= p) == 0)
					F[i][j] = 1;
				t2 += F[i][j];
			}

			t2++;
			if (t2 < Constants.TOTFREQ)
				F[i][M] += Constants.TOTFREQ - t2;
			else
				F[i][M] -= t2 - Constants.TOTFREQ;

			// Store frequency table
			// i
			if (rle_i != 0) {
				rle_i--;
			} else {
				cp.put((byte) i);
				// FIXME: could use order-0 statistics to observe which alphabet
				// symbols are present and base RLE on that ordering instead.
				if (i != 0 && T[i - 1] != 0) {
					for (rle_i = i + 1; rle_i < 256 && T[rle_i] != 0; rle_i++)
						;
					rle_i -= i + 1;
					cp.put((byte) rle_i);
				}
			}

			int[] F_i_ = F[i];
			x = 0;
			rle_j = 0;
			for (j = 0; j < 256; j++) {
				if (F_i_[j] != 0) {
					// fprintf(stderr, "F[%d][%d]=%d, x=%d\n", i, j, F_i_[j],
					// x);

					// j
					if (rle_j != 0) {
						rle_j--;
					} else {
						cp.put((byte) j);
						if (rle_j == 0 && j != 0 && F_i_[j - 1] != 0) {
							for (rle_j = j + 1; rle_j < 256 && F_i_[rle_j] != 0; rle_j++)
								;
							rle_j -= j + 1;
							cp.put((byte) rle_j);
						}
					}

					// F_i_[j]
					if (F_i_[j] < 128) {
						cp.put((byte) F_i_[j]);
					} else {
						cp.put((byte) (128 | (F_i_[j] >> 8)));
						cp.put((byte) (F_i_[j] & 0xff));
					}

					Encoding.RansEncSymbolInit(syms[i][j], x, F_i_[j],
							Constants.TF_SHIFT);
					x += F_i_[j];
				}
			}
			cp.put((byte) 0);
		}
		cp.put((byte) 0);

		tab_size = cp.position();
		assert (tab_size < 257 * 257 * 3);

		int rans0, rans1, rans2, rans3;
		rans0 = Constants.RANS_BYTE_L;
		rans1 = Constants.RANS_BYTE_L;
		rans2 = Constants.RANS_BYTE_L;
		rans3 = Constants.RANS_BYTE_L;

		ByteBuffer ptr = cp.slice();

		int isz4 = in_size >> 2;
		int i0 = 1 * isz4 - 2;
		int i1 = 2 * isz4 - 2;
		int i2 = 3 * isz4 - 2;
		int i3 = 4 * isz4 - 2;

		int l0 = 0xFF & in.get(i0 + 1);
		int l1 = 0xFF & in.get(i1 + 1);
		int l2 = 0xFF & in.get(i2 + 1);
		int l3 = 0xFF & in.get(i3 + 1);

		// Deal with the remainder
		l3 = 0xFF & in.get(in_size - 1);
		for (i3 = in_size - 2; i3 > 4 * isz4 - 2; i3--) {
			int c3 = 0xFF & in.get(i3);
			rans3 = Encoding.RansEncPutSymbol(rans3, ptr, syms[c3][l3]);
			l3 = c3;
		}

		for (; i0 >= 0; i0--, i1--, i2--, i3--) {
			int c0 = 0xFF & in.get(i0);
			int c1 = 0xFF & in.get(i1);
			int c2 = 0xFF & in.get(i2);
			int c3 = 0xFF & in.get(i3);

			rans3 = Encoding.RansEncPutSymbol(rans3, ptr, syms[c3][l3]);
			rans2 = Encoding.RansEncPutSymbol(rans2, ptr, syms[c2][l2]);
			rans1 = Encoding.RansEncPutSymbol(rans1, ptr, syms[c1][l1]);
			rans0 = Encoding.RansEncPutSymbol(rans0, ptr, syms[c0][l0]);

			l0 = c0;
			l1 = c1;
			l2 = c2;
			l3 = c3;
		}

		rans3 = Encoding.RansEncPutSymbol(rans3, ptr, syms[0][l3]);
		rans2 = Encoding.RansEncPutSymbol(rans2, ptr, syms[0][l2]);
		rans1 = Encoding.RansEncPutSymbol(rans1, ptr, syms[0][l1]);
		rans0 = Encoding.RansEncPutSymbol(rans0, ptr, syms[0][l0]);

		ByteOrder byteOrder = out_buf.order();
		out_buf.order(ByteOrder.LITTLE_ENDIAN);
		ptr.putInt(rans3);
		ptr.putInt(rans2);
		ptr.putInt(rans1);
		ptr.putInt(rans0);
		ptr.flip();
		int cdata_size = ptr.limit();
		ByteBufferUtils.reverse(ptr);

		// Finalise block size and return it
		out_buf.limit(1 + 4 + 4 + tab_size + cdata_size);
		out_buf.put(0, (byte) 1);
		out_buf.putInt(1, out_buf.limit() - 5);
		out_buf.putInt(5, in_size);
		out_buf.order(byteOrder);
		out_buf.position(0);
		return out_buf;
	}
}
