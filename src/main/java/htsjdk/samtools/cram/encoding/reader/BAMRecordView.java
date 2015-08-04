/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package htsjdk.samtools.cram.encoding.reader;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

public class BAMRecordView {
	public static final int BLOCK_SIZE = 0;
	public static final int REF_ID = 4;
	public static final int POS = 8;
	public static final int READ_NAME_LEN = 12;
	public static final int MAP_Q = 13;
	public static final int INDEX_BIN = 14;
	public static final int CIGAR_LEN = 16;
	public static final int FLAGS = 18;
	public static final int READ_LEN = 20;
	public static final int MATE_REF_ID = 24;
	public static final int MATE_AL_START = 28;
	public static final int INS_SIZE = 32;
	public static final int READ_NAME = 36;

	private static final long MAX_UINT = ((long) Integer.MAX_VALUE * 2) + 1;

	private int CIGAR = -1;
	private int BASES = -1;
	private int SCORES = -1;
	private int TAGS = -1;
	public int END = -1;

	private byte[] buf;

	/**
	 * Pointer to the beginning of the currently written record.
	 */
	int start;

	public BAMRecordView(byte[] buf) {
		this.buf = buf;
	}

	public void setData(byte[] buf) {
		this.buf = buf;
		reset();
	}

	public void reset() {
		start = 0;
		CIGAR = -1;
		BASES = -1;
		SCORES = -1;
		TAGS = -1;
		END = -1;
	}

	public void setRefID(int id) {
		writeInt(id, REF_ID);
	}

	public int getRefID() {
		return getInt(REF_ID);
	}

	public void setAlignmentStart(int alignmenStart) {
		writeInt(alignmenStart - 1, POS);
	}

	public void setReadName(String readName) {
		writeUByte((short) (readName.length() + 1), READ_NAME_LEN);
		CIGAR = READ_NAME + wrteZString(readName, READ_NAME);
	}

	public boolean isReadNameSet() {
		return CIGAR > -1;
	}

	public void setReadName(byte[] readName) {
		writeUByte((short) (readName.length + 1), READ_NAME_LEN);
		System.arraycopy(readName, 0, buf, start + READ_NAME, readName.length);
		buf[start + READ_NAME + readName.length] = 0;
		CIGAR = READ_NAME + readName.length + 1;
	}

	public void setMappingScore(int score) {
		writeUByte((short) score, MAP_Q);
	}

	public void setIndexBin(int bin) {
		writeUShort(bin, INDEX_BIN);
	}

	public void setCigar(Cigar cigar) {
		if (CIGAR < 0)
			throw new RuntimeException("Premature setting of cigar.");
		writeUShort(cigar.numCigarElements(), CIGAR_LEN);
		final int[] binaryCigar = encodeBinaryCigar(cigar);
		int at = CIGAR;
		for (final int cigarElement : binaryCigar) {
			// Assumption that this will fit into an integer, despite the fact
			// that it is specced as a uint.
			writeInt(cigarElement, at);
			at += 4;
		}
		BASES = at;
	}

	public void setFlags(int flags) {
		// writeInt(flags, FLAGS);
		writeUShort(flags, FLAGS);
	}

	public void setReadLength(int readLength) {
		writeInt(readLength, READ_LEN);
	}

	public int getReadLength() {
		return getInt(READ_LEN);
	}

	public void setMateRefID(int mateRefID) {
		writeInt(mateRefID, MATE_REF_ID);
	}

	public int getMateRefID() {
		return getInt(MATE_REF_ID);
	}

	public void setMateAlStart(int mateAlStart) {
		writeInt(mateAlStart - 1, MATE_AL_START);
	}

	public void setInsertSize(int insertSize) {
		writeInt(insertSize, INS_SIZE);
	}

	public void setBases(byte[] bases, int offset, int length) {
		if (BASES < 0)
			throw new RuntimeException("Premature setting of bases.");

		int i;

		for (i = 1; i < length; i += 2)
			buf[start + BASES + i / 2] = (byte) (charToCompressedBaseHigh(bases[offset + i - 1]) | charToCompressedBaseLow(bases[offset
					+ i]));

		// Last nybble
		if (i == length)
			buf[start + BASES + i / 2] = charToCompressedBaseHigh((char) bases[offset + i - 1]);

		setReadLength(length);
		SCORES = BASES + length / 2 + length % 2;
	}

	public void setBases(byte[] bases) {
		setBases(bases, 0, bases.length);
	}

	public void setQualityScores(byte[] qualities, int offset, int length) {
		if (SCORES < 0)
			throw new RuntimeException("Premature setting of scores.");

		if (length == 0) {
			int len = getReadLength();
			for (int i = 0; i < len; i++)
				buf[start + SCORES + i] = (byte) 0xFF;
			TAGS = SCORES + len;
		} else {
			System.arraycopy(qualities, offset, buf, start + SCORES, length);
			TAGS = SCORES + length;
		}
	}

	public void setQualityScores(byte[] qualities) {
		if (SCORES < 0)
			throw new RuntimeException("Premature setting of scores.");

		if (qualities.length == 0) {
			int len = getReadLength();
			for (int i = 0; i < len; i++)
				buf[start + SCORES + i] = (byte) 0xFF;
			TAGS = SCORES + len;
		} else {
			System.arraycopy(qualities, 0, buf, start + SCORES, qualities.length);
			TAGS = SCORES + qualities.length;
		}
	}

	public void addTag(int id, byte[] data, int offset, int len) {
		if (TAGS < 0)
			throw new RuntimeException("Premature addition of tag.");
		if (END < 0)
			END = TAGS;

		buf[start + END++] = (byte) (id & 0xFF);
		buf[start + END++] = (byte) ((id >> 8) & 0xFF);
		buf[start + END++] = (byte) ((id >> 16) & 0xFF);
		System.arraycopy(data, offset, buf, start + END, len);
		END += len;
	}

	public void setTagData(byte[] data, int offset, int length) {
		if (TAGS < 0)
			throw new RuntimeException("Premature addition of tag.");
		if (END < 0)
			END = TAGS;

		try {
			System.arraycopy(data, offset, buf, start + TAGS, length);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw e;
		}
		END += length;
	}

	public int finish() {
		int blockSize = END;
		if (blockSize < 0)
			blockSize = TAGS;
		if (blockSize < 0)
			throw new RuntimeException("Incomplete record.");

		writeInt(blockSize - 4, BLOCK_SIZE);

		position(start + END);

		return blockSize;
	}

	public void position(int start) {
		this.start = start;
		CIGAR = -1;
		BASES = -1;
		SCORES = -1;
		TAGS = -1;
		END = -1;
	}

	public int position() {
		return start;
	}

	private final void writeInt(final int value, final int at) {
		buf[start + at] = (byte) (value & 0xFF);
		buf[start + at + 1] = (byte) ((value >> 8) & 0xFF);
		buf[start + at + 2] = (byte) ((value >> 16) & 0xFF);
		buf[start + at + 3] = (byte) ((value >> 24) & 0xFF);
	}

	private final int getInt(final int at) {
		int value = (0xFF & buf[start + at]) | ((0xFF & buf[start + at + 1]) << 8)
				| ((0xFF & buf[start + at + 2]) << 16) | ((0xFF & buf[start + at + 3]) << 24);
		return value;
	}

	private int writeUInt(Long value, int at) {
		if (value < 0) {
			throw new IllegalArgumentException("Negative value (" + value + ") passed to unsigned writing method.");
		}
		if (value > MAX_UINT) {
			throw new IllegalArgumentException("Value (" + value + ") to large to be written as uint.");
		}

		buf[start + at] = (byte) (value & 0xFF);
		buf[start + at + 1] = (byte) ((value >> 8) & 0xFF);
		buf[start + at + 2] = (byte) ((value >> 16) & 0xFF);
		buf[start + at + 3] = (byte) ((value >> 24) & 0xFF);
		return 4;
	}

	private long getUInt(int at) {
		long value = 0;
		for (int i = start + at; i < at + 5; i++)
			value |= 0xFF & buf[i];

		return value;
	}

	private final int wrteZString(final String value, final int at) {
		value.getBytes(0, value.length(), buf, start + at);
		buf[start + at + value.length()] = 0;
		return value.length() + 1;
	}

	private void writeUByte(short value, int at) {
		buf[start + at] = (byte) (value & 0xFF);
	}

	private int getUByte(int at) {
		return buf[start + at] & 0xFF;
	}

	private byte writeByte(int value, int at) {
		buf[start + at] = (byte) value;
		return 1;
	}

	private final void writeUShort(final int value, final int at) {
		buf[start + at] = (byte) (value & 0xFF);
		buf[start + at + 1] = (byte) ((value >> 8) & 0xFF);
	}

	private final int getUShort(int at) {
		int value = buf[start + at + 1] & 0xFF;
		value <<= 8;
		value |= buf[start + at] & 0xFF;
		return value;
	}

	private final int writeShort(final short value, final int at) {
		buf[start + at] = (byte) (value & 0xFF);
		buf[start + at + 1] = (byte) ((value >> 8) & 0xFF);
		return 2;
	}

	/**
	 * Convert from a byte array containing =AaCcGgTtNn represented as ASCII, to
	 * a byte array half as long, with =, A, C, G, T converted to 0, 1, 2, 4, 8,
	 * 15.
	 * 
	 * @param readBases
	 *            Bases as ASCII bytes.
	 * @return New byte array with bases represented as nybbles, in BAM binary
	 *         format.
	 */
	private static byte[] bytesToCompressedBases(final byte[] readBases) {
		final byte[] compressedBases = new byte[(readBases.length + 1) / 2];
		int i;
		for (i = 1; i < readBases.length; i += 2) {
			compressedBases[i / 2] = (byte) (charToCompressedBaseHigh(readBases[i - 1]) | charToCompressedBaseLow(readBases[i]));
		}
		// Last nybble
		if (i == readBases.length) {
			compressedBases[i / 2] = charToCompressedBaseHigh((char) readBases[i - 1]);
		}
		return compressedBases;
	}

	/**
	 * Convert from ASCII byte to BAM nybble representation of a base in
	 * high-order nybble.
	 * 
	 * @param base
	 *            One of =AaCcGgTtNn.
	 * @return High-order nybble-encoded equivalent.
	 */
	private static byte charToCompressedBaseHigh(final int base) {
		switch (base) {
		case '=':
			return COMPRESSED_EQUAL_HIGH;
		case 'a':
		case 'A':
			return COMPRESSED_A_HIGH;
		case 'c':
		case 'C':
			return COMPRESSED_C_HIGH;
		case 'g':
		case 'G':
			return COMPRESSED_G_HIGH;
		case 't':
		case 'T':
			return COMPRESSED_T_HIGH;
		case 'n':
		case 'N':
		case '.':
			return COMPRESSED_N_HIGH;

			// IUPAC ambiguity codes
		case 'M':
		case 'm':
			return COMPRESSED_M_HIGH;
		case 'R':
		case 'r':
			return COMPRESSED_R_HIGH;
		case 'S':
		case 's':
			return COMPRESSED_S_HIGH;
		case 'V':
		case 'v':
			return COMPRESSED_V_HIGH;
		case 'W':
		case 'w':
			return COMPRESSED_W_HIGH;
		case 'Y':
		case 'y':
			return COMPRESSED_Y_HIGH;
		case 'H':
		case 'h':
			return COMPRESSED_H_HIGH;
		case 'K':
		case 'k':
			return COMPRESSED_K_HIGH;
		case 'D':
		case 'd':
			return COMPRESSED_D_HIGH;
		case 'B':
		case 'b':
			return COMPRESSED_B_HIGH;
		default:
			throw new IllegalArgumentException("Bad  byte passed to charToCompressedBase: " + base);
		}
	}

	/**
	 * Convert from BAM nybble representation of a base in low-order nybble to
	 * ASCII byte.
	 * 
	 * @param base
	 *            One of COMPRESSED_*_LOW, a low-order nybble encoded base.
	 * @return ASCII base, one of ACGTN=.
	 */
	private static byte compressedBaseToByteLow(final int base) {
		switch (base & 0xf) {
		case COMPRESSED_EQUAL_LOW:
			return '=';
		case COMPRESSED_A_LOW:
			return 'A';
		case COMPRESSED_C_LOW:
			return 'C';
		case COMPRESSED_G_LOW:
			return 'G';
		case COMPRESSED_T_LOW:
			return 'T';
		case COMPRESSED_N_LOW:
			return 'N';

			// IUPAC ambiguity codes
		case COMPRESSED_M_LOW:
			return 'M';
		case COMPRESSED_R_LOW:
			return 'R';
		case COMPRESSED_S_LOW:
			return 'S';
		case COMPRESSED_V_LOW:
			return 'V';
		case COMPRESSED_W_LOW:
			return 'W';
		case COMPRESSED_Y_LOW:
			return 'Y';
		case COMPRESSED_H_LOW:
			return 'H';
		case COMPRESSED_K_LOW:
			return 'K';
		case COMPRESSED_D_LOW:
			return 'D';
		case COMPRESSED_B_LOW:
			return 'B';

		default:
			throw new IllegalArgumentException("Bad  byte passed to charToCompressedBase: " + base);
		}
	}

	/**
	 * Convert from ASCII byte to BAM nybble representation of a base in
	 * low-order nybble.
	 * 
	 * @param base
	 *            One of =AaCcGgTtNn.
	 * @return Low-order nybble-encoded equivalent.
	 */
	private static byte charToCompressedBaseLow(final int base) {
		switch (base) {
		case '=':
			return COMPRESSED_EQUAL_LOW;
		case 'a':
		case 'A':
			return COMPRESSED_A_LOW;
		case 'c':
		case 'C':
			return COMPRESSED_C_LOW;
		case 'g':
		case 'G':
			return COMPRESSED_G_LOW;
		case 't':
		case 'T':
			return COMPRESSED_T_LOW;
		case 'n':
		case 'N':
		case '.':
			return COMPRESSED_N_LOW;

			// IUPAC ambiguity codes
		case 'M':
		case 'm':
			return COMPRESSED_M_LOW;
		case 'R':
		case 'r':
			return COMPRESSED_R_LOW;
		case 'S':
		case 's':
			return COMPRESSED_S_LOW;
		case 'V':
		case 'v':
			return COMPRESSED_V_LOW;
		case 'W':
		case 'w':
			return COMPRESSED_W_LOW;
		case 'Y':
		case 'y':
			return COMPRESSED_Y_LOW;
		case 'H':
		case 'h':
			return COMPRESSED_H_LOW;
		case 'K':
		case 'k':
			return COMPRESSED_K_LOW;
		case 'D':
		case 'd':
			return COMPRESSED_D_LOW;
		case 'B':
		case 'b':
			return COMPRESSED_B_LOW;
		default:
			throw new IllegalArgumentException("Bad  byte passed to charToCompressedBase: " + base);
		}
	}

	private static final byte COMPRESSED_EQUAL_LOW = 0;
	private static final byte COMPRESSED_A_LOW = 1;
	private static final byte COMPRESSED_C_LOW = 2;
	private static final byte COMPRESSED_M_LOW = 3;
	private static final byte COMPRESSED_G_LOW = 4;
	private static final byte COMPRESSED_R_LOW = 5;
	private static final byte COMPRESSED_S_LOW = 6;
	private static final byte COMPRESSED_V_LOW = 7;
	private static final byte COMPRESSED_T_LOW = 8;
	private static final byte COMPRESSED_W_LOW = 9;
	private static final byte COMPRESSED_Y_LOW = 10;
	private static final byte COMPRESSED_H_LOW = 11;
	private static final byte COMPRESSED_K_LOW = 12;
	private static final byte COMPRESSED_D_LOW = 13;
	private static final byte COMPRESSED_B_LOW = 14;
	private static final byte COMPRESSED_N_LOW = 15;
	private static final byte COMPRESSED_EQUAL_HIGH = COMPRESSED_EQUAL_LOW << 4;
	private static final byte COMPRESSED_A_HIGH = COMPRESSED_A_LOW << 4;
	private static final byte COMPRESSED_C_HIGH = COMPRESSED_C_LOW << 4;
	private static final byte COMPRESSED_G_HIGH = COMPRESSED_G_LOW << 4;
	private static final byte COMPRESSED_T_HIGH = (byte) (COMPRESSED_T_LOW << 4);
	private static final byte COMPRESSED_N_HIGH = (byte) (COMPRESSED_N_LOW << 4);

	private static final byte COMPRESSED_M_HIGH = (byte) (COMPRESSED_M_LOW << 4);
	private static final byte COMPRESSED_R_HIGH = (byte) (COMPRESSED_R_LOW << 4);
	private static final byte COMPRESSED_S_HIGH = (byte) (COMPRESSED_S_LOW << 4);
	private static final byte COMPRESSED_V_HIGH = (byte) (COMPRESSED_V_LOW << 4);
	private static final byte COMPRESSED_W_HIGH = (byte) (COMPRESSED_W_LOW << 4);
	private static final byte COMPRESSED_Y_HIGH = (byte) (COMPRESSED_Y_LOW << 4);
	private static final byte COMPRESSED_H_HIGH = (byte) (COMPRESSED_H_LOW << 4);
	private static final byte COMPRESSED_K_HIGH = (byte) (COMPRESSED_K_LOW << 4);
	private static final byte COMPRESSED_D_HIGH = (byte) (COMPRESSED_D_LOW << 4);
	private static final byte COMPRESSED_B_HIGH = (byte) (COMPRESSED_B_LOW << 4);

	public int getSequenceId() {
		return getInt(REF_ID);
	}

	public int getAlignmentStart() {
		return getInt(POS);
	}

	public int getFlags() {
		return getUShort(FLAGS);
	}

	// Representation of CigarOperator in BAM file
	private static final byte OP_M = 0;
	private static final byte OP_I = 1;
	private static final byte OP_D = 2;
	private static final byte OP_N = 3;
	private static final byte OP_S = 4;
	private static final byte OP_H = 5;
	private static final byte OP_P = 6;
	private static final byte OP_EQ = 7;
	private static final byte OP_X = 8;

	public int calculateAlignmentEnd() {
		int aend = getAlignmentStart() - 1;

		int readNamelen = getUByte(READ_NAME_LEN);
		int cigarLen = getUShort(CIGAR_LEN);

		CIGAR = READ_NAME + readNamelen;

		for (int i = CIGAR; i < CIGAR + 4 * cigarLen; i += 4) {
			int e = getInt(i);
			byte op = (byte) (e & 15);
			int oplen = e >>> 4;

			switch (op) {
			case OP_M:
			case OP_D:
			case OP_N:
			case OP_EQ:
			case OP_X:
				aend += oplen;
				break;
			case OP_I:
			case OP_S:
			case OP_H:
			case OP_P:
				break;

			default:
				break;
			}
		}
		return aend;
	}

	/**
	 * From htsjdk
	 * @param cigar
	 * @return
	 */
	static int[] encodeBinaryCigar(final Cigar cigar) {
		if (cigar.numCigarElements() == 0) {
			return new int[0];
		}

		final int[] binaryCigar = new int[cigar.numCigarElements()];
		int binaryCigarLength = 0;
		for (int i = 0; i < cigar.numCigarElements(); ++i) {
			final CigarElement cigarElement = cigar.getCigarElement(i);
			final int op = CigarOperator.enumToBinary(cigarElement.getOperator());
			binaryCigar[binaryCigarLength++] = cigarElement.getLength() << 4 | op;
		}
		return binaryCigar;
	}
}
