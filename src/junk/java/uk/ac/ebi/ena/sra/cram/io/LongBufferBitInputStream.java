/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
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
package uk.ac.ebi.ena.sra.cram.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class LongBufferBitInputStream extends DataInputStream implements BitInputStream {
	private int nofBufferedBits = 0; // 0-31 at first, expandable to 60
	private long byteBuffer = 0; // 32/60-bit
	private boolean endOfStream = false;
	private boolean throwEOF = false;

	public LongBufferBitInputStream(InputStream in) {
		this(in, true);
	}

	public LongBufferBitInputStream(InputStream in, boolean throwEOF) {
		super(in);
		this.throwEOF = throwEOF;
	}

	public final boolean readBit() throws IOException { // Read 1 bit
		if (--nofBufferedBits >= 0)
			return ((byteBuffer >>> nofBufferedBits) & 1) == 1; // leftmost bit
		nofBufferedBits = -1; // 7
		for (int i = 0; i < 4 && !endOfStream; i++) { // read f (formerly 7)
														// bytes into buffer, or
														// until end of stream
			int buf = in.read();
			if (buf == -1) {
				endOfStream = true;
			} else {
				byteBuffer = (byteBuffer << 8) | (buf);
				nofBufferedBits += 8;
			}
		}
		if (endOfStream && nofBufferedBits == 0 && throwEOF)
			throw new EOFException("End of stream.");

		return ((byteBuffer >>> nofBufferedBits) & 1) == 1;
	}

	public final int readBits(int n) throws IOException {
		if (n == 0)
			return 0;
		long x = 0;
		while (n > nofBufferedBits && !endOfStream) {
			n -= nofBufferedBits;
			x |= rightBitsLong(nofBufferedBits, byteBuffer) << n; // use all
																	// buffered
																	// bytes
			nofBufferedBits = 0; // buffer now empty
			for (int i = 0; i < 4 && !endOfStream; i++) { // read 4 (formerly 7)
															// bytes into
															// buffer, or until
															// end of stream
				int buf = in.read();
				if (buf == -1) {
					endOfStream = true;
				} else {
					byteBuffer = (byteBuffer << 8) | (buf);
					nofBufferedBits += 8;
				}
			}
		}
		if (endOfStream && nofBufferedBits < n && throwEOF)
			throw new EOFException("End of stream.");
		nofBufferedBits -= n;
		x = (x | rightBitsLong(n, byteBuffer >>> nofBufferedBits));
		return (int) x;
	}

	@Override
	public final boolean putBack(long n, int numBits) {
		if (numBits == 0)
			return true;

		if ((nofBufferedBits + numBits) > 60)
			throw new RuntimeException("Buffer full.");

		long masked = (0xFF >>> (8 - numBits)) & n;
		byteBuffer = (byteBuffer << numBits) | masked;

		nofBufferedBits += numBits;

		return true;
	}

	private static final int rightBits(int n, int x) {
		return x & ((1 << n) - 1);
	}

	private static final long rightBitsLong(int n, long x) {
		return x & ((1L << n) - 1);
	}

	public final long readLongBits(int len) throws IOException {
		if (len > 64)
			throw new RuntimeException("More then 64 bits are requested in one read from bit stream.");

		long result = 0;
		final long last = len - 1;
		for (long bi = 0; bi <= last; bi++) {
			final boolean frag = readBit();
			if (frag)
				result |= 1L << (last - bi);
		}
		return result;
	}

	public void reset() {
		nofBufferedBits = 0;
		byteBuffer = 0;
	}

	@Override
	public boolean endOfStream() throws IOException {
		return endOfStream;
	}

	public int getNofBufferedBits() {
		return nofBufferedBits;
	}

	/*
	 * NOT TESTED!!! (non-Javadoc)
	 * 
	 * @see uk.ac.ebi.ena.sra.cram.io.BitInputStream#alignToByte()
	 */
	@Override
	public void alignToByte() throws IOException {
		int bitsToSkip = nofBufferedBits % 8;

		nofBufferedBits -= bitsToSkip;

		long mask = -1L >>> (64 - (nofBufferedBits - bitsToSkip));
		byteBuffer = byteBuffer & mask;
	}

	@Override
	public int readAlignedBytes(byte[] array) throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public boolean ensureMarker(long marker, int nofBits) throws IOException {
		throw new RuntimeException("Not implemented.");
	}

}
