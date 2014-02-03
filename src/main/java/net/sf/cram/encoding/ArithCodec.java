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
package net.sf.cram.encoding;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.common.NullOutputStream;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import SevenZip.Compression.LZMA.Decoder;
import SevenZip.Compression.LZMA.Encoder;

public class ArithCodec extends AbstractBitCodec<byte[]> {
	private byte curBit = 0;
	private int curByte = 0;
	private double min = 0;
	private double max = 1;
	private double localMin = 0;
	private double localMax = 1;

	private final int TERMINATOR = 256;
	private double[] probs;
	private int[] map, rev_map;
	private long bitCount;

	private ByteArrayOutputStream baos;
	private ArrayList<Integer> fileData;

	public ArithCodec(int[] freqs, int[] map) {
		// build expanded map ------------------------------
		this.map = new int[257]; // ASCII + end character
		Arrays.fill(this.map, -1);
		for (int i = 0; i < map.length; i++)
			this.map[map[i]] = i;
		this.map[this.TERMINATOR] = map.length;

		// copy collapsed map, plus end character ----------
		this.rev_map = new int[map.length + 1];
		System.arraycopy(map, 0, this.rev_map, 0, map.length);
		this.rev_map[map.length] = this.TERMINATOR;

		// build probability table from frequency count ----
		this.probs = new double[freqs.length + 1];
		int total = 0, endCharCount = 0;
		for (int i = 0; i < freqs.length; i++)
			total += freqs[i];
		endCharCount = (total / 100) > 0 ? (total / 100) : (total / 10);
		total += endCharCount;
		int t = 0;
		for (int i = 0; i < freqs.length; i++) {
			t += freqs[i];
			this.probs[i] = (double) t / (double) total;
		}
		this.probs[this.probs.length - 1] = 1.0;

		// initialize byte stream --------------------------
		this.baos = new ByteArrayOutputStream(2 * 215000000);
		this.fileData = new ArrayList();
	}

	/*
	 * Reading and expanding a bit stream based on given frequency count
	 */
	@Override
	public byte[] read(BitInputStream bis) throws IOException {
		this.baos.reset();
		this.fileData.clear();
		curBit = 0;
		curByte = 0;
		min = 0;
		max = 1;
		localMin = 0;
		localMax = 1;

		int read = decodeCharacter(bis);
		while (read != this.map[this.TERMINATOR]) {
			this.baos.write(this.rev_map[read]);
			read = decodeCharacter(bis);
		}

		return this.baos.toByteArray();
	}

	public int decodeCharacter(BitInputStream bis) throws IOException {
		double tempMin = min;
		double tempMax = max;
		byte tempBit = curBit;
		int tempByte = curByte;
		int val = 0;
		if (this.fileData.isEmpty())
			fileData.add(bis.readBits(8));
		while (true) {
			double cur = (min + max) / 2.0;
			val = -1;
			for (int i = 0; i < probs.length; i++) {
				if (probs[i] > min) {
					if (probs[i] > max)
						val = i;
					break;
				}
			}
			if (val == -1) {
				boolean bit = false;
				if ((fileData.get(curByte) & (128 >> curBit)) != 0)
					bit = true;
				if (bit)
					min = cur;
				else
					max = cur;
				curBit++;
				if (curBit == 8) {
					curBit = 0;
					curByte++;
					if (curByte > fileData.size() - 1) {
						try {
							fileData.add(bis.readBits(8));
						} catch (Throwable t) {
							fileData.add(0);
						}
					}
				}
			} else
				break;
		}
		min = tempMin;
		max = tempMax;
		curBit = tempBit;
		curByte = tempByte;
		while (true) {
			double cur = (min + max) / 2.0;
			int temp = 0;
			for (; temp < probs.length; temp++)
				if (probs[temp] > cur)
					break;
			if (cur < 0 || cur > 1)
				temp = -1;
			if (temp != val) {
				boolean bit = false;
				if ((fileData.get(curByte) & (128 >> curBit)) != 0)
					bit = true;
				if (bit)
					min = cur;
				else
					max = cur;
				curBit++;
				if (curBit == 8) {
					curBit = 0;
					curByte++;
					if (curByte > fileData.size() - 1)
						try {
							fileData.add(bis.readBits(8));
						} catch (Throwable t) {
							fileData.add(0);
						}
				}
			} else {
				tempMin = 0;
				if (val > 0)
					tempMin = probs[val - 1];
				double factor = 1.0 / (probs[val] - tempMin);
				min = factor * (min - tempMin);
				max = factor * (max - tempMin);
				break;
			}
		}
		return val;
	}

	/*
	 * Write compressed output to a bit stream
	 */
	@Override
	public long write(BitOutputStream bos, byte[] object) throws IOException {
		this.baos.reset();
		curBit = 0;
		curByte = 0;
		min = 0;
		max = 1;
		localMin = 0;
		localMax = 1;
		this.bitCount = 0;

		try {
			for (int i = 0; i < object.length; i++)
				encodeCharacter(bos, this.map[object[i] & 0xFF]);
			encodeCharacter(bos, this.map[this.TERMINATOR]);
			encodeCharacter(bos, this.map[this.TERMINATOR]);
			flush(bos);
		} catch (Exception ex) {
			Log.getInstance(getClass()).error(ex);
		}

		return this.bitCount;
	}

	private void encodeCharacter(BitOutputStream bos, int character) throws Exception {
		if (probs.length < 2 || probs[probs.length - 1] != 1 || character < 0 || character >= probs.length)
			throw new Exception("Invalid input");
		if (character > 0)
			localMin = probs[character - 1];
		else
			localMin = 0;
		localMax = probs[character];
		while (true) {
			double cur = (min + max) / 2.0;
			if (cur < localMin) {
				curByte |= (128 >> curBit); // set bit = 1, left-to-right
				curBit++;
				if (curBit == 8) {
					bos.write(curByte, 8);
					curByte = 0; // byte containing bits to be written
					curBit = 0; // bit-position, left-to-right
					this.bitCount += 8;
				}
				min = cur; // wrote 1 (go higher) adjust min
			} else if (cur >= localMax) {
				curBit++;
				if (curBit == 8) {
					bos.write(curByte, 8);
					curByte = 0;
					curBit = 0;
					this.bitCount += 8;
				}
				max = cur; // wrote 0 (go lower) adjust max
			} else {
				double factor = 1.0 / (localMax - localMin);
				min = factor * (min - localMin);
				max = factor * (max - localMin);
				break;
			}
		}
	}

	private void flush(BitOutputStream bos) throws IOException {
		if (curBit != 0) {
			while (true) {
				while (true) {
					double cur = (min + max) / 2.0;
					double mid = (localMin + localMax) / 2.0;
					if (cur < mid) {
						curByte |= (128 >> curBit);
						min = cur;
					} else
						max = cur;
					curBit++;
					if (curBit == 8) {
						bos.write(curByte, 8);
						curByte = 0;
						curBit = 0;
						this.bitCount += 8;
						break;
					}
				}
				double cur = (min + max) / 2.0;
				if (cur >= localMin && cur < localMax)
					break;
			}
		}
		bos.close();
	}

	/*
	 * Compress and count bits in the end
	 */
	@Override
	public long numberOfBits(byte[] object) {
		NullOutputStream baos = new NullOutputStream();
		DefaultBitOutputStream nBos = new DefaultBitOutputStream(baos);

		this.baos.reset();
		curBit = 0;
		curByte = 0;
		min = 0;
		max = 1;
		localMin = 0;
		localMax = 1;
		this.bitCount = 0;

		try {
			for (int i = 0; i < object.length; i++)
				encodeCharacter(nBos, this.map[object[i] & 0xFF]);
			encodeCharacter(nBos, this.map[this.TERMINATOR]);
			encodeCharacter(nBos, this.map[this.TERMINATOR]);
			flush(nBos);
		} catch (Exception ex) {
			Log.getInstance(ArithCodec.class).error(ex);
		}

		return this.bitCount;
	}

	@Override
	public byte[] read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	private static class AC_params {
		int[] freqs;
		int[] map;
		byte[] data;
	}

	private static AC_params getAC_params(String filePath) {
		AC_params p = new AC_params();
		SAMFileReader reader = new SAMFileReader(new File(filePath));
		// int[] values =
		for (SAMRecord record : reader) {

		}

		return p;
	}

	public static void main(String[] args) throws IOException {
		int[] freqs = { 10000000, 5000000, 1000000, 2500000, 3000000 };
		int[] map = { 0, 1, 2, 3, 4 };

		int len = 0;
		for (int i = 0; i < map.length; i++)
			len += freqs[i];
		System.out.printf("%d distinct values, data size=%d\n", map.length, len);

		byte[] data = new byte[len];
		int k = 0;
		for (int i = 0; i < map.length; i++)
			for (int j = 0; j < freqs[i]; j++)
				data[k++] = (byte) (map[i] & 0xFF);

		shuffle(data);

		{
			ArithCodec codec = new ArithCodec(freqs, map);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			BitOutputStream bos = new DefaultBitOutputStream(baos);

			long startTime = System.nanoTime();
			long writtenBits = codec.write(bos, data);
			codec.flush(bos);
			bos.close();
			long endTime = System.nanoTime();
			float ms = (endTime - startTime) / 1000000f;
			System.out.printf("write: b/b %.2f, time %.2fms, %.2f mb/s\n", ((float) writtenBits) / data.length, ms,
					((float) len) / 1024 / 1024 / ms * 1000);

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			BitInputStream bis = new DefaultBitInputStream(bais);
			startTime = System.nanoTime();
			byte[] readData = codec.read(bis);
			endTime = System.nanoTime();
			ms = (endTime - startTime) / 1000000f;
			System.out.printf("read: time %.2fms, %.2f mb/s\n", ms, ((float) len) / 1024 / 1024 / ms * 1000);

			assertArrayEquals(data, readData);
		}

		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GZIPOutputStream gos = new GZIPOutputStream(baos);
			long startTime = System.nanoTime();
			gos.write(data);
			gos.close();
			long endTime = System.nanoTime();
			float ms = (endTime - startTime) / 1000000f;
			System.out.printf("gzip write: b/b %.2f, time %.2fms, %.2f mb/s\n", (8f * baos.size()) / data.length, ms,
					((float) len) / 1024 / 1024 / ms * 1000);

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			GZIPInputStream gis = new GZIPInputStream(bais);
			byte[] readData = new byte[data.length];
			startTime = System.nanoTime();
			gis.read(readData);
			endTime = System.nanoTime();
			ms = (endTime - startTime) / 1000000f;
			System.out.printf("gzip read: time %.2fms, %.2f mb/s\n", ms, ((float) len) / 1024 / 1024 / ms * 1000);

		}

		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Encoder e = new Encoder();
			ByteArrayOutputStream propStream = new ByteArrayOutputStream();
			e.WriteCoderProperties(propStream);
			byte[] propArray = propStream.toByteArray();
			System.out.println("lzma prop data size " + propArray.length);

			long startTime = System.nanoTime();
			e.Code(new ByteArrayInputStream(data), baos, data.length, -1, null);
			long endTime = System.nanoTime();
			float ms = (endTime - startTime) / 1000000f;
			System.out.printf("lzma write: b/b %.2f, time %.2fms, %.2f mb/s\n", (8f * baos.size()) / data.length, ms,
					((float) len) / 1024 / 1024 / ms * 1000);

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			baos = new ByteArrayOutputStream();
			Decoder d = new Decoder();
			d.SetDecoderProperties(propArray);
			startTime = System.nanoTime();
			d.Code(bais, baos, data.length);
			endTime = System.nanoTime();
			ms = (endTime - startTime) / 1000000f;
			System.out.printf("lzma read: time %.2fms, %.2f mb/s\n", ms, ((float) len) / 1024 / 1024 / ms * 1000);

		}
	}

	private static void shuffle(byte[] data) {
		Random random = new Random();
		byte tmp;
		int rPos;
		for (int i = 0; i < data.length; i++) {
			tmp = data[i];
			rPos = random.nextInt(data.length);
			data[i] = data[rPos];
			data[rPos] = tmp;
		}
	}
}
