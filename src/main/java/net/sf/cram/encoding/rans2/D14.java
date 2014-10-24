package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import net.sf.cram.encoding.rans2.Decoding.FC;
import net.sf.cram.encoding.rans2.Decoding.RansDecSymbol;
import net.sf.cram.encoding.rans2.Decoding.ari_decoder;

class D14 {
	private static void readStats(ByteBuffer cp, ari_decoder[] D,
			RansDecSymbol[][] syms) {
		int i, j = -999, x, rle_i = 0, rle_j;
		i = 0xFF & cp.get();
		do {
			rle_j = x = 0;
			j = 0xFF & cp.get();
			if (D[i] == null)
				D[i] = new ari_decoder();
			do {
				if (D[i].fc[j] == null)
					D[i].fc[j] = new FC();
				if ((D[i].fc[j].F = (0xFF & cp.get())) >= 128) {
					D[i].fc[j].F &= ~128;
					D[i].fc[j].F = ((D[i].fc[j].F & 127) << 8)
							| (0xFF & cp.get());
				}
				D[i].fc[j].C = x;

				if (D[i].fc[j].F == 0)
					D[i].fc[j].F = Constants.TOTFREQ;

				if (syms[i][j] == null)
					syms[i][j] = new RansDecSymbol();

				Decoding.RansDecSymbolInit(syms[i][j], D[i].fc[j].C,
						D[i].fc[j].F);

				/* Build reverse lookup table */
				if (D[i].R == null)
					D[i].R = new byte[Constants.TOTFREQ];
				Arrays.fill(D[i].R, x, x + D[i].fc[j].F, (byte) j);
				// memset(&D[i].R[x], j, D[i].fc[j].F);

				x += D[i].fc[j].F;
				assert (x <= Constants.TOTFREQ);

				if (rle_j == 0 && j + 1 == (0xFF & cp.get(cp.position()))) {
					j = (0xFF & cp.get());
					rle_j = (0xFF & cp.get());
				} else if (rle_j != 0) {
					rle_j--;
					j++;
				} else {
					j = (0xFF & cp.get());
				}
			} while (j != 0);

			if (rle_i == 0 && i + 1 == (0xFF & cp.get(cp.position()))) {
				i = (0xFF & cp.get());
				rle_i = (0xFF & cp.get());
			} else if (rle_i != 0) {
				rle_i--;
				i++;
			} else {
				i = (0xFF & cp.get());
			}
		} while (i != 0);
	}

	static ByteBuffer decode(ByteBuffer in, ByteBuffer out_buf) {
		in.order(ByteOrder.LITTLE_ENDIAN);
		int in_sz = in.getInt();
		int out_sz = in.getInt();
		if (out_buf == null)
			out_buf = ByteBuffer.allocate(out_sz);
		else
			out_buf.limit(out_sz);
		if (out_buf.remaining() < out_sz)
			throw new RuntimeException("Output buffer too small to fit "
					+ out_sz + " bytes.");

		/* Load in the static tables */
		ByteBuffer cp = in.slice();
		ari_decoder[] D = new ari_decoder[256];
		RansDecSymbol[][] syms = new RansDecSymbol[256][256];
		for (int i = 0; i < syms.length; i++)
			for (int j = 0; j < syms[i].length; j++)
				syms[i][j] = new RansDecSymbol();
		readStats(cp, D, syms);

		// Precompute reverse lookup of frequency.

		int rans0, rans1, rans2, rans7;
		ByteBuffer ptr = cp.slice();
		ptr.order(ByteOrder.LITTLE_ENDIAN);
		rans0 = ptr.getInt();
		rans1 = ptr.getInt();
		rans2 = ptr.getInt();
		rans7 = ptr.getInt();

		int isz4 = out_sz >> 2;
		int i0 = 0 * isz4;
		int i1 = 1 * isz4;
		int i2 = 2 * isz4;
		int i7 = 3 * isz4;
		int l0 = 0;
		int l1 = 0;
		int l2 = 0;
		int l7 = 0;
		for (; i0 < isz4; i0++, i1++, i2++, i7++) {
			int c0 = 0xFF & D[l0].R[Decoding.RansDecGet(rans0,
					Constants.TF_SHIFT)];
			int c1 = 0xFF & D[l1].R[Decoding.RansDecGet(rans1,
					Constants.TF_SHIFT)];
			int c2 = 0xFF & D[l2].R[Decoding.RansDecGet(rans2,
					Constants.TF_SHIFT)];
			int c7 = 0xFF & D[l7].R[Decoding.RansDecGet(rans7,
					Constants.TF_SHIFT)];

			out_buf.put(i0, (byte) c0);
			out_buf.put(i1, (byte) c1);
			out_buf.put(i2, (byte) c2);
			out_buf.put(i7, (byte) c7);

			rans0 = Decoding.RansDecAdvanceSymbolStep(rans0, syms[l0][c0],
					Constants.TF_SHIFT);
			rans1 = Decoding.RansDecAdvanceSymbolStep(rans1, syms[l1][c1],
					Constants.TF_SHIFT);
			rans2 = Decoding.RansDecAdvanceSymbolStep(rans2, syms[l2][c2],
					Constants.TF_SHIFT);
			rans7 = Decoding.RansDecAdvanceSymbolStep(rans7, syms[l7][c7],
					Constants.TF_SHIFT);

			rans0 = Decoding.RansDecRenorm(rans0, ptr);
			rans1 = Decoding.RansDecRenorm(rans1, ptr);
			rans2 = Decoding.RansDecRenorm(rans2, ptr);
			rans7 = Decoding.RansDecRenorm(rans7, ptr);

			l0 = c0;
			l1 = c1;
			l2 = c2;
			l7 = c7;
		}

		// Remainder
		for (; i7 < out_sz; i7++) {
			int c7 = 0xFF & D[l7].R[Decoding.RansDecGet(rans7,
					Constants.TF_SHIFT)];
			out_buf.put(i7, (byte) c7);
			rans7 = Decoding.RansDecAdvanceSymbol(rans7, ptr, syms[l7][c7],
					Constants.TF_SHIFT);
			l7 = c7;
		}

		return out_buf;
	}
}
