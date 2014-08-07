package net.sf.cram.encoding.rANS;

import java.nio.ByteBuffer;

import net.sf.cram.io.ByteBufferUtils;

class FrequencyTable {
	public static final int[] Order0_FT(ByteBuffer data, int TOTFREQ) {
		int[] F = new int[256];
		int T = 0;
		while (data.hasRemaining()) {
			F[data.get() & 0xFF]++;
			T++;
		}

		if (true)
			return F;

		// Normalise so T[i] == 65536
		int n, j;
		for (n = j = 0; j < 256; j++)
			if (F[j] != 0)
				n++;

		for (j = 0; j < 256; j++) {
			if (F[j] == 0)
				continue;
			if ((F[j] *= ((double) TOTFREQ - n) / T) == 0)
				F[j] = 1;
		}
		return F;
	}

	public static void write(int[] freqs, ByteBuffer out) {
		for (int i = 0; i < freqs.length; i++) {
			if (freqs[i] == 0)
				continue;

			int j = i + 1;
			int len = 0;
			while (i > 0 && freqs[i - 1] != 0 && j < freqs.length && freqs[j++] > 0)
				len++;

			out.put((byte) i);
			switch (len) {
			case 0:
				if (i > 0 && freqs[i - 1] != 0)
					out.put((byte) 0);
				ByteBufferUtils.writeUnsignedITF8(freqs[i], out);
				break;
			case 1:
				out.put((byte) 1);
				for (j = i; j <= i + len; j++)
					ByteBufferUtils.writeUnsignedITF8(freqs[j], out);

				break;
			default:
				out.put((byte) len);
				for (j = i; j <= i + len; j++)
					ByteBufferUtils.writeUnsignedITF8(freqs[j], out);
				break;
			}
			i += len;
		}
		out.put((byte) 0);
	}

	// @formatter:off
//	  41 0a  45 81 f4  46 02     14  1e  7f 5a 02  00
//	   A 10   E  500    F +2(GH) 20  30 127 Z   2  END
	// @formatter:on

	public static void read(int[] freqs, ByteBuffer in) {
		int prevV = Integer.MIN_VALUE;
		while (in.hasRemaining()) {
			int v = 0xFF & in.get();
			if (v == 0)
				break;

			if (v - prevV == 1) {
				int len = ByteBufferUtils.readUnsignedITF8(in);
				if (len == 0)
					freqs[v] = ByteBufferUtils.readUnsignedITF8(in);
				else
					for (int i = 0; i <= len; i++)
						freqs[v + i] = ByteBufferUtils.readUnsignedITF8(in);
			} else
				freqs[v] = ByteBufferUtils.readUnsignedITF8(in);

			prevV = v;
		}
	}

	public static final int[][] Order1_FT(ByteBuffer data, int TOTFREQ, int slices, int[] T) {
		int[][] F = new int[256][256];

		// Normalise so T[i] == 65536
		int j, i, last, c;
		int in_size = data.remaining();
		for (last = i = 0; i < in_size; i++) {
			c = data.get() & 0xFF;
			F[last][c]++;
			T[last]++;
			last = c;
		}

		for (int s = 1; s < slices; s++) {
			F[0][0xFF & data.get(s * (in_size / slices))]++;
		}
		T[0] += slices - 1;

		// Normalise so T[i] == 65536
		for (i = 0; i < 256; i++) {
			int t = T[i], t2, n;

			if (t == 0)
				continue;

			for (n = j = 0; j < 256; j++)
				if (F[i][j] != 0)
					n++;

			for (t2 = j = 0; j < 256; j++) {
				if (F[i][j] == 0)
					continue;
				if ((F[i][j] *= ((double) TOTFREQ - n) / t) == 0)
					F[i][j] = 1;
				t2 += F[i][j];
			}

			assert (t2 <= TOTFREQ);
		}
		return F;
	}
}
