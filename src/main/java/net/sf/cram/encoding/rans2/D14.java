package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.encoding.rans2.Decoding.RansDecSymbol;
import net.sf.cram.encoding.rans2.Decoding.ari_decoder;

class D14 {
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
		Freqs.readStats_o1(cp, D, syms);

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
			// rans7 = Decoding.RansDecRenorm(rans7, ptr);
			l7 = c7;
		}

		return out_buf;
	}
}
