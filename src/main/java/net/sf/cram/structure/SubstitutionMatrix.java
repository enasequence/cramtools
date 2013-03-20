package net.sf.cram.structure;

import java.util.Arrays;
import java.util.Comparator;

public class SubstitutionMatrix {
	public static final byte[] BASES = new byte[] { 'A', 'C', 'G',  'N', 'T'};
	public static final byte[] BASES_LC = new byte[] { 'a', 'c', 'g',  'n', 't'};
	private byte[] bytes = new byte[5];
	private byte[][] codes = new byte[255][255];
	private byte[][] bases = new byte[255][255];

	public SubstitutionMatrix(long[][] freqs) {
		for (int i = 0; i < BASES.length; i++) {
			bytes[i] = rank(BASES[i], freqs[BASES[i]]);
		}
		
		for (int i=0; i<bases.length; i++) 
			Arrays.fill(bases[i], (byte)'N') ;
		
		for (int i=0; i<BASES.length; i++) {
			byte r = BASES[i] ;
			for (byte b:BASES) {
				if (r == b) continue;
				bases[r][codes[r][b]] = b ;
				bases[BASES_LC[i]][codes[r][b]] = b ;
			}
		}
	}

	public SubstitutionMatrix(byte[] matrix) {
		this.bytes = matrix ;
		for (int i = 0; i < 5; i++) {
			byte rank = matrix[i];
			int baseIndex = BASES.length-1;
			for (int r = 0; r < 4; r ++) {
				byte code = (byte) ((rank >>> 2*r) & 3);
				if (baseIndex == i)
					baseIndex--;
				codes[BASES[i]][BASES[baseIndex--]] = code;
			}
		}
		
		for (int i=0; i<bases.length; i++) 
			Arrays.fill(bases[i], (byte)'N') ;
		
		for (int i=0; i<BASES.length; i++) {
			byte r = BASES[i] ;
			for (byte b:BASES) {
				if (r == b) continue;
				bases[r][codes[r][b]] = b ;
				bases[BASES_LC[i]][codes[r][b]] = b ;
			}
		}
	}
	
	public byte[] getEncodedMatrix () {
		return bytes ;
	}

	private static class SubCode {
		byte ref, base;
		long freq;
		byte rank;

		public SubCode(byte ref, byte base, long freq) {
			super();
			this.ref = ref;
			this.base = base;
			this.freq = freq;
		}

		byte code;
	}

	private static Comparator<SubCode> comparator = new Comparator<SubstitutionMatrix.SubCode>() {

		@Override
		public int compare(SubCode o1, SubCode o2) {
			if (o1.freq != o2.freq)
				return (int) (o2.freq - o1.freq);
			return o1.base - o2.base;
		}
	};

	private byte rank(byte refBase, long[] freqs) {
		SubCode[] subCodes = new SubCode[4];
		{
			int i = 0;
			for (byte base : BASES) {
				if (refBase == base)
					continue;
				subCodes[i++] = new SubCode(refBase, base, freqs[base]);
			}
		}

		Arrays.sort(subCodes, comparator);

		for (byte i = 0; i < subCodes.length; i++)
			subCodes[i].rank = i;

		for (byte i = 0; i < subCodes.length; i++)
			subCodes[i].freq = 0;

		Arrays.sort(subCodes, comparator);

		byte rank = 0;
		for (byte i = 0; i < subCodes.length; i++) {
			rank <<= 2;
			rank |= subCodes[i].rank;
		}

		{
			int i = 0;
			for (byte base : BASES) {
				if (refBase == base)
					continue;
				codes[refBase][base] = subCodes[i++].rank;
			}
		}

		return rank;
	}

	public byte code(byte refBase, byte readBase) {
		return codes[refBase][readBase];
	}
	
	public byte base(byte refBase, byte code) {
		return bases[refBase][code];
	}

	public static void main(String[] args) {
		SubstitutionMatrix m = new SubstitutionMatrix(new byte[] { 27, (byte) 228, 27,
				27, 27 });

		for (byte refBase : BASES) {
			for (byte base : BASES) {
				if (refBase == base)
					continue;
				System.out.printf("Ref=%c, base=%c, code=%d\n", (char)refBase, (char)base, m.code(refBase, base));
			}
		}
		System.out.println(Arrays.toString(m.bytes));
		System.out.println("===============================================");
		
		
		long[][] freqs = new long[255][255] ;
		for (int r=0; r<BASES.length; r++) {
			for (int b=0; b<BASES.length; b++) {
				if (r == b) continue ;
				freqs[BASES[r]][BASES[b]] = b ;
			}
		}
		
		m = new SubstitutionMatrix(freqs) ;
		for (byte refBase : BASES) {
			for (byte base : BASES) {
				if (refBase == base)
					continue;
				System.out.printf("Ref=%c, base=%c, code=%d\n", (char)refBase, (char)base, m.code(refBase, base));
			}
		}
		System.out.println(Arrays.toString(m.bytes));
	}
}
