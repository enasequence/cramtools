package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class D04 {
	static ByteBuffer decode(ByteBuffer in) {
		/* Load in the static tables */
		in.order(ByteOrder.LITTLE_ENDIAN);

		int in_sz = in.getInt();
		int out_sz = in.getInt();
		ByteBuffer out_buf = ByteBuffer.allocate(out_sz);
		ByteBuffer cp = in.slice();
		cp.order(ByteOrder.LITTLE_ENDIAN);
		Decoding.ari_decoder D = new Decoding.ari_decoder();
		Decoding.RansDecSymbol[] syms = new Decoding.RansDecSymbol[256];
		for (int i = 0; i < syms.length; i++)
			syms[i] = new Decoding.RansDecSymbol();

		Freqs.readStats_o0(cp, D, syms);

		int rans0, rans1, rans2, rans3;
		ByteBuffer ptr = cp;
		rans0 = ptr.getInt();
		rans1 = ptr.getInt();
		rans2 = ptr.getInt();
		rans3 = ptr.getInt();

		int out_end = (out_sz & ~3);
		for (int i = 0; i < out_end; i += 4) {
			byte c0 = D.R[Decoding.RansDecGet(rans0, Constants.TF_SHIFT)];
			byte c1 = D.R[Decoding.RansDecGet(rans1, Constants.TF_SHIFT)];
			byte c2 = D.R[Decoding.RansDecGet(rans2, Constants.TF_SHIFT)];
			byte c3 = D.R[Decoding.RansDecGet(rans3, Constants.TF_SHIFT)];

			out_buf.put(i + 0, c0);
			out_buf.put(i + 1, c1);
			out_buf.put(i + 2, c2);
			out_buf.put(i + 3, c3);

			rans0 = Decoding.RansDecAdvanceSymbolStep(rans0, syms[0xFF & c0],
					Constants.TF_SHIFT);
			rans1 = Decoding.RansDecAdvanceSymbolStep(rans1, syms[0xFF & c1],
					Constants.TF_SHIFT);
			rans2 = Decoding.RansDecAdvanceSymbolStep(rans2, syms[0xFF & c2],
					Constants.TF_SHIFT);
			rans3 = Decoding.RansDecAdvanceSymbolStep(rans3, syms[0xFF & c3],
					Constants.TF_SHIFT);

			rans0 = Decoding.RansDecRenorm(rans0, ptr);
			rans1 = Decoding.RansDecRenorm(rans1, ptr);
			rans2 = Decoding.RansDecRenorm(rans2, ptr);
			rans3 = Decoding.RansDecRenorm(rans3, ptr);
		}

		out_buf.position(out_end);
		byte c;
		switch (out_sz & 3) {
		case 0:
			break;
		case 1:
			c = D.R[Decoding.RansDecGet(rans0, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans0, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);
			break;

		case 2:
			c = D.R[Decoding.RansDecGet(rans0, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans0, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);

			c = D.R[Decoding.RansDecGet(rans1, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans1, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);
			break;

		case 3:
			c = D.R[Decoding.RansDecGet(rans0, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans0, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);

			c = D.R[Decoding.RansDecGet(rans1, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans1, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);

			c = D.R[Decoding.RansDecGet(rans2, Constants.TF_SHIFT)];
			Decoding.RansDecAdvanceSymbol(rans2, ptr, syms[0xFF & c],
					Constants.TF_SHIFT);
			out_buf.put(c);
			break;
		}

		out_buf.position(0);
		return out_buf;
	}
}
