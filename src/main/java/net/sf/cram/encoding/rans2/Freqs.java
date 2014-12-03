package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.sf.cram.encoding.rans2.Decoding.FC;
import net.sf.cram.encoding.rans2.Decoding.RansDecSymbol;
import net.sf.cram.encoding.rans2.Decoding.ari_decoder;

class Freqs {

	static void readStats_o0(ByteBuffer cp, ari_decoder D, RansDecSymbol[] syms) {
		// Precompute reverse lookup of frequency.
		int rle = 0;
		int x = 0;
		int j = cp.get() & 0xFF;
		do {
			if (D.fc[j] == null)
				D.fc[j] = new Decoding.FC();
			if ((D.fc[j].F = (cp.get() & 0xFF)) >= 128) {
				D.fc[j].F &= ~128;
				D.fc[j].F = ((D.fc[j].F & 127) << 8) | (cp.get() & 0xFF);
			}
			D.fc[j].C = x;

			// System.out.printf("i=%d j=%d F=%d C=%d\n", i, j, D.fc[j].F,
			// D.fc[j].C);

			Decoding.RansDecSymbolInit(syms[j], D.fc[j].C, D.fc[j].F);

			/* Build reverse lookup table */
			if (D.R == null)
				D.R = new byte[Constants.TOTFREQ];
			Arrays.fill(D.R, x, x + D.fc[j].F, (byte) j);

			x += D.fc[j].F;

			if (rle == 0 && j + 1 == (0xFF & cp.get(cp.position()))) {
				j = cp.get() & 0xFF;
				rle = cp.get() & 0xFF;
			} else if (rle != 0) {
				rle--;
				j++;
			} else {
				j = cp.get() & 0xFF;
			}
			// System.out.println("j=" + j);
		} while (j != 0);

		assert (x < Constants.TOTFREQ);
	}

	static void readStats_o1(ByteBuffer cp, ari_decoder[] D,
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

}
