package net.sf.cram.encoding.rANS;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import net.sf.cram.ref.ReferenceSource;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMIterator;
import net.sf.samtools.SAMRecord;

public class rANS_decode0_4way {
	private static final int TF_SHIFT = 12;
	private static final int TOTFREQ = (1 << TF_SHIFT);
	private static final int RANS_BYTE_L = 1 << 23;

	private static class FC {
		int F, C;
	}

	private static class ari_decoder {
		FC[] fc = new FC[256];
		byte[] R;
	}

	private static class RansDecSymbol {
		int start; // Start of range.
		int freq; // Symbol frequency.
	}

	// Initialize a decoder symbol to start "start" and frequency "freq"
	private static void RansDecSymbolInit(RansDecSymbol s, int start, int freq) {
		assert (start <= (1 << 16));
		assert (freq <= (1 << 16) - start);
		s.start = start;
		s.freq = freq;
	}

	// Advances in the bit stream by "popping" a single symbol with range start
	// "start" and frequency "freq". All frequencies are assumed to sum to
	// "1 << scale_bits".
	// No renormalization or output happens.
	private static int RansDecAdvanceStep(int r, int start, int freq, int scale_bits) {
		int mask = ((1 << scale_bits) - 1);

		// s, x = D(x)
		return freq * (r >> scale_bits) + (r & mask) - start;
	}

	// Equivalent to RansDecAdvanceStep that takes a symbol.
	static int RansDecAdvanceSymbolStep(int r, RansDecSymbol sym, int scale_bits) {
		return RansDecAdvanceStep(r, sym.start, sym.freq, scale_bits);
	}

	// Returns the current cumulative frequency (map it to a symbol yourself!)
	static int RansDecGet(int r, int scale_bits) {
		return r & ((1 << scale_bits) - 1);
	}

	// Equivalent to RansDecAdvance that takes a symbol.
	static void RansDecAdvanceSymbol(int r, ByteBuffer pptr, RansDecSymbol sym, int scale_bits) {
		RansDecAdvance(r, pptr, sym.start, sym.freq, scale_bits);
	}

	// Advances in the bit stream by "popping" a single symbol with range start
	// "start" and frequency "freq". All frequencies are assumed to sum to
	// "1 << scale_bits",
	// and the resulting bytes get written to ptr (which is updated).
	static int RansDecAdvance(int r, ByteBuffer pptr, int start, int freq, int scale_bits) {
		int mask = (1 << scale_bits) - 1;

		// s, x = D(x)
		r = freq * (r >> scale_bits) + (r & mask) - start;

		// renormalize
		if (r < RANS_BYTE_L) {
			do {
				final int b = 0xFF & pptr.get();
				r = (r << 8) | b;
			} while (r < RANS_BYTE_L);

		}

		return r;
	}

	ByteBuffer rans_uncompress_O0(ByteBuffer in) {
		/* Load in the static tables */
		in.order(ByteOrder.LITTLE_ENDIAN);
		int out_sz = in.getInt();
		ByteBuffer out_buf = ByteBuffer.allocate(out_sz);
		ByteBuffer cp = in.slice();
		cp.order(ByteOrder.LITTLE_ENDIAN);
		int i, j, x, rle;
		ari_decoder D = new ari_decoder();
		RansDecSymbol[] syms = new RansDecSymbol[256];
		for (i = 0; i < syms.length; i++)
			syms[i] = new RansDecSymbol();
		i = 0;

		// Precompute reverse lookup of frequency.
		rle = x = 0;
		j = cp.get() & 0xFF;
		do {
			if (D.fc[j] == null)
				D.fc[j] = new FC();
			if ((D.fc[j].F = (cp.get() & 0xFF)) >= 128) {
				D.fc[j].F &= ~128;
				D.fc[j].F = ((D.fc[j].F & 127) << 8) | (cp.get() & 0xFF);
			}
			D.fc[j].C = x;

			// System.out.printf("i=%d j=%d F=%d C=%d\n", i, j, D.fc[j].F,
			// D.fc[j].C);

			RansDecSymbolInit(syms[j], D.fc[j].C, D.fc[j].F);

			/* Build reverse lookup table */
			if (D.R == null)
				D.R = new byte[TOTFREQ];
			Arrays.fill(D.R, x, x + D.fc[j].F, (byte) j);

			x += D.fc[j].F;

			if (rle == 0 && j + 1 == cp.get(cp.position())) {
				j = cp.get();
				rle = cp.get();
			} else if (rle != 0) {
				rle--;
				j++;
			} else {
				j = cp.get();
			}
			// System.out.println("j=" + j);
		} while (j != 0);

		assert (x < TOTFREQ);

		int rans0, rans1, rans2, rans3;
		ByteBuffer ptr = cp;
		rans0 = rans_byte.RansDecInit(ptr);
		rans1 = rans_byte.RansDecInit(ptr);
		rans2 = rans_byte.RansDecInit(ptr);
		rans3 = rans_byte.RansDecInit(ptr);

		int out_end = (out_sz & ~3);
		// System.out.println("out_end=" + out_end);
		for (i = 0; i < out_end; i += 4) {
			// System.out.printf("rans[0-3]=%d %d %d %d\n", rans0, rans1, rans2,
			// rans3);
			byte c0 = D.R[rans_byte.RansDecGet(rans0, TF_SHIFT)];
			byte c1 = D.R[rans_byte.RansDecGet(rans1, TF_SHIFT)];
			byte c2 = D.R[rans_byte.RansDecGet(rans2, TF_SHIFT)];
			byte c3 = D.R[rans_byte.RansDecGet(rans3, TF_SHIFT)];

			out_buf.put(i + 0, c0);
			out_buf.put(i + 1, c1);
			out_buf.put(i + 2, c2);
			out_buf.put(i + 3, c3);
			// System.out.printf("c[0-3]=%d %d %d %d\n", c0, c1, c2, c3);

			rans0 = RansDecAdvanceSymbolStep(rans0, syms[c0], TF_SHIFT);
			rans1 = RansDecAdvanceSymbolStep(rans1, syms[c1], TF_SHIFT);
			rans2 = RansDecAdvanceSymbolStep(rans2, syms[c2], TF_SHIFT);
			rans3 = RansDecAdvanceSymbolStep(rans3, syms[c3], TF_SHIFT);

			rans0 = rans_byte.RansDecRenorm(rans0, ptr);
			rans1 = rans_byte.RansDecRenorm(rans1, ptr);
			rans2 = rans_byte.RansDecRenorm(rans2, ptr);
			rans3 = rans_byte.RansDecRenorm(rans3, ptr);
		}

		out_buf.position(out_end);
		byte c;
		switch (out_sz & 3) {
		case 0:
			break;
		case 1:
			c = D.R[RansDecGet(rans0, TF_SHIFT)];
			RansDecAdvanceSymbol(rans0, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);
			break;

		case 2:
			c = D.R[RansDecGet(rans0, TF_SHIFT)];
			RansDecAdvanceSymbol(rans0, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);

			c = D.R[RansDecGet(rans1, TF_SHIFT)];
			RansDecAdvanceSymbol(rans1, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);
			break;

		case 3:
			c = D.R[RansDecGet(rans0, TF_SHIFT)];
			RansDecAdvanceSymbol(rans0, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);

			c = D.R[RansDecGet(rans1, TF_SHIFT)];
			RansDecAdvanceSymbol(rans1, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);

			c = D.R[RansDecGet(rans2, TF_SHIFT)];
			RansDecAdvanceSymbol(rans2, ptr, syms[c], TF_SHIFT);
			out_buf.put(c);
			break;
		}

		out_buf.position(0);
		return out_buf;
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);
		InputStream is = rANS_decode0_4way.class.getClassLoader().getResourceAsStream("data/v30_lzma.cram");

		ReferenceSource referenceSource = new ReferenceSource(new File("src/test/resources/data/ref.fa"));
		SAMIterator si = new SAMIterator(is, referenceSource);
		while (si.hasNext()) {
			SAMRecord record = si.next();
		}
	}
}
