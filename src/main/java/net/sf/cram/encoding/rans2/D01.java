package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class D01 {
	public static ByteBuffer decode(ByteBuffer in) {
		/* Load in the static tables */
		in.order(ByteOrder.LITTLE_ENDIAN);

		int in_sz = in.getInt();
		int out_sz = in.getInt();
		ByteBuffer out_buf = ByteBuffer.allocate(out_sz);
		ByteBuffer cp = in.slice();
		cp.order(ByteOrder.LITTLE_ENDIAN);
		int i, j, x, rle;
		Decoding.ari_decoder D = new Decoding.ari_decoder();
		Decoding.RansDecSymbol[] syms = new Decoding.RansDecSymbol[256];
		for (i = 0; i < syms.length; i++)
			syms[i] = new Decoding.RansDecSymbol();
		i = 0;

		// Precompute reverse lookup of frequency.
		rle = x = 0;
		j = cp.get() & 0xFF;
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

		int rans0;
		ByteBuffer ptr = cp;
		rans0 = ptr.getInt();

		int out_end = out_sz;
		// System.out.println("out_end=" + out_end);
		for (i = 0; i < out_end; i++) {
			// System.out.printf("rans[0-3]=%d %d %d %d\n", rans0, rans1, rans2,
			// rans3);
			byte c0 = D.R[Decoding.RansDecGet(rans0, Constants.TF_SHIFT)];

			out_buf.put(i + 0, c0);
			// System.out.printf("c[0-3]=%d %d %d %d\n", c0, c1, c2, c3);

			rans0 = Decoding.RansDecAdvanceSymbolStep(rans0, syms[0xFF & c0],
					Constants.TF_SHIFT);

			rans0 = Decoding.RansDecRenorm(rans0, ptr);
		}

		out_buf.position(0);
		return out_buf;
	}
}
