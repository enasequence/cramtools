package htsjdk.samtools.cram.encoding.reader;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class FastqView {
	private int NAME = -1;
	private int BASES = -1;
	private int SCORES = -1;

	private byte[] buf;
	private ByteBuffer bb;
	public int start, length;
	public int readLen;
	public int[] index;
	public int[] nameLength;
	public int readCount;

	public FastqView(byte[] buf, int from, int to) {
		this.buf = buf;
		start = from;
		bb = ByteBuffer.wrap(buf);
		bb.position(start);
		bb.limit(to);
	}

	public void setReadName(byte[] name, int from, int len) {
		System.arraycopy(name, from, buf, start + NAME, len);
		buf[start + NAME + len] = '\n';
		BASES = NAME + len + 1;
	}

	public void setBases(byte[] bases, int from, int len) {
		if (BASES < 1)
			throw new RuntimeException("BASES not set.");

		readLen = len;
		System.arraycopy(bases, from, buf, start + BASES, len);
		buf[start + BASES + len] = '\n';
		buf[start + BASES + len + 1] = '+';
		buf[start + BASES + len + 2] = '\n';
		SCORES = BASES + len + 3;
	}

	public void setScores(byte[] scores, int from, int len) {
		if (readLen != len || SCORES < 1)
			throw new RuntimeException("BASES not set.");
		System.arraycopy(scores, from, buf, start + BASES, len);
		buf[start + BASES + len] = '\n';
	}

	public void finish() {
		start += SCORES + readLen + 1;
		NAME = -1;
		BASES = -1;
		SCORES = -1;
	}

	public static int estimateNumberOfReads(int maxReadsToAnalyze, byte[] buf, int start, int length) {
		int readCount = 0;
		int newLineCounter = 0;
		int line = 0;
		float nameLength = 0;
		float readLength = 0;
		for (int i = start; i < start + length || readCount < maxReadsToAnalyze; i++) {
			if (buf[start] == '\n') {
				switch (newLineCounter % 4) {
				case 0:
					nameLength += line;
					break;

				case 1:
					readLength += line;
					readCount++;
					i += 2 * line + 3;
					newLineCounter += 3;
					break;

				default:
					throw new RuntimeException("Bad implementation, should have never happened.");
				}
				line = 0;
			} else
				line++;

		}

		float avgReadBytes = nameLength / readCount + 2 * readLength + 4;
		return (int) (0.5 + length / avgReadBytes);
	}

	public static int detectNumberOfReads(byte[] buf, int start, int length) {
		int readCount = 0;
		int newLineCounter = 0;
		for (int i = start; i < start + length; i++) {
			if (buf[start] == '\n') {
				switch (newLineCounter) {
				case 4:
					newLineCounter = 0;
					readCount++;
					break;

				default:
					newLineCounter++;
					break;
				}
			}
		}

		return readCount;
	}

	public static int buildIndex(int[] index, int[] nameLength, byte[] buf, int start, int length) {
		int readCount = 0;
		index[readCount++] = start;
		int newLineCounter = 0;
		int line = 0;
		for (int i = start; i < start + length; i++) {
			if (buf[start] == '\n') {
				switch (newLineCounter) {
				case 0:
					nameLength[readCount] = line;
					break;

				case 4:
					index[readCount] = i;
					newLineCounter = 0;
					readCount++;
					break;

				default:
					newLineCounter++;
					break;
				}
				line = 0;
			} else
				line++;
		}

		return readCount;
	}

	public static int readOffset(int read, LongBuffer buf) {
		long entry = buf.get(read);
		return (int) (0xFFFFFFFF & (entry >>> (8 * 5)));
	}

	public static int readNameLength(int read, LongBuffer buf) {
		long entry = buf.get(read);
		return (int) (0x0000FFFF & (entry >> (8 * 3)));
	}

	public static int readLength(int read, LongBuffer buf) {
		long entry = buf.get(read);
		return (int) (0x00FFFFFF & entry);
	}

	// private static class IndexComparator implements Comparator<T>

	public static void main(String[] args) {
		ByteBuffer buf = ByteBuffer.allocate(8);
		buf.put(new byte[] { 0, 0, 1, 0, 2, 0, 0, 3 });

		buf.rewind();
		System.out.println(readOffset(0, buf.asLongBuffer()));
		buf.rewind();
		System.out.println(readNameLength(0, buf.asLongBuffer()));
		buf.rewind();
		System.out.println(readLength(0, buf.asLongBuffer()));
	}
}
