package net.sf.cram.encoding.huffint;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.cram.ReadTag;
import net.sf.cram.encoding.CanonicalHuffmanIntegerCodec;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.stats.CompressionHeaderFactory;
import net.sf.cram.stats.CompressionHeaderFactory.HuffmanParamsCalculator;

class Helper {
	TreeMap<Integer, HuffmanBitCode> codes;

	int[] values, bitLengths;
	private TreeMap<Integer, SortedSet<Integer>> codebook;

	final HuffmanBitCode[] sortedCodes;
	final HuffmanBitCode[] sortedByValue;
	final int[] sortedValues;
	final int[] sortedBitCodes;
	final int[] sortedValuesByBitCode;
	final int[] sortedBitLensByBitCode;
	final int[] bitCodeToValue;

	Helper(int[] values, int[] bitLengths) {
		this.values = values;
		this.bitLengths = bitLengths;

		buildCodeBook();
		buildCodes();

		ArrayList<HuffmanBitCode> list = new ArrayList<HuffmanBitCode>(
				codes.size());
		list.addAll(codes.values());
		Collections.sort(list, bitCodeComparator);
		sortedCodes = (HuffmanBitCode[]) list.toArray(new HuffmanBitCode[list
				.size()]);

		System.out.println("Sorted codes:");
		for (HuffmanBitCode code : sortedCodes)
			System.out.println(code);

		sortedValues = Arrays.copyOf(values, values.length);
		Arrays.sort(sortedValues);
		System.out.println("Sorted values:");
		for (int value : sortedValues)
			System.out.println(value);

		{
			int i = 0;
			sortedByValue = new HuffmanBitCode[sortedValues.length];
			for (int value : sortedValues)
				sortedByValue[i++] = codes.get(value);
		}

		System.out.println("Sorted by value:");
		for (HuffmanBitCode code : sortedByValue)
			System.out.println(code);

		sortedBitCodes = new int[sortedCodes.length];
		sortedValuesByBitCode = new int[sortedCodes.length];
		sortedBitLensByBitCode = new int[sortedCodes.length];
		int maxBitCode = 0;
		for (int i = 0; i < sortedBitCodes.length; i++) {
			sortedBitCodes[i] = sortedCodes[i].bitCode;
			sortedValuesByBitCode[i] = sortedCodes[i].value;
			sortedBitLensByBitCode[i] = sortedCodes[i].bitLentgh;
			if (maxBitCode < sortedCodes[i].bitCode)
				maxBitCode = sortedCodes[i].bitCode;
		}

		bitCodeToValue = new int[maxBitCode + 1];
		Arrays.fill(bitCodeToValue, -1);
		for (int i = 0; i < sortedBitCodes.length; i++) {
			bitCodeToValue[sortedCodes[i].bitCode] = i;
		}
	}

	private void buildCodeBook() {
		codebook = new TreeMap<Integer, SortedSet<Integer>>();
		for (int i = 0; i < values.length; i++) {
			if (codebook.containsKey(bitLengths[i]))
				((TreeSet) codebook.get(bitLengths[i])).add(values[i]);
			else {
				TreeSet<Integer> entry = new TreeSet<Integer>();
				entry.add(values[i]);
				codebook.put(bitLengths[i], entry);
			}
		}
	}

	private void buildCodes() {
		codes = new TreeMap<Integer, HuffmanBitCode>();
		Set keySet = codebook.keySet();
		int codeLength = 0, codeValue = -1;
		for (Object key : keySet) { // Iterate over code lengths
			int iKey = Integer.parseInt(key.toString());

			TreeSet<Integer> get = (TreeSet<Integer>) codebook.get(key);
			for (Integer entry : get) { // Iterate over symbols
				HuffmanBitCode code = new HuffmanBitCode();
				code.bitLentgh = iKey; // given: bit length
				code.value = entry; // given: symbol

				codeValue++; // increment bit value by 1
				int delta = iKey - codeLength; // new length?
				codeValue = codeValue << delta; // pad with 0's
				code.bitCode = codeValue; // calculated: huffman code
				codeLength += delta; // adjust current code len

				if (NumberOfSetBits(codeValue) > iKey)
					throw new IllegalArgumentException("Symbol out of range");

				codes.put(entry, code); // Store HuffmanBitCode

			}

		}
	}

	final long write(final BitOutputStream bos, final int value) throws IOException {
		int index = Arrays.binarySearch(sortedValues, value);
		HuffmanBitCode code = sortedByValue[index];
		if (code.value != value)
			throw new RuntimeException(String.format(
					"Searching for %d but found %s.", value, code.toString()));
		bos.write(code.bitCode, code.bitLentgh);
		// System.out.println("Writing: " + code.toString());
		return code.bitLentgh;
	}

	private HuffmanBitCode searchCode = new HuffmanBitCode();

	final int read(final BitInputStream bis) throws IOException {
		int prevLen = 0;
		int bits = 0;
		for (int i = 0; i < sortedCodes.length; i++) {
			int len = sortedCodes[i].bitLentgh;
			bits <<= len - prevLen;
			bits |= bis.readBits(len - prevLen);
			prevLen = len;

			/*
			 * Variant 1: searchCode.bitCode = bits; searchCode.bitLentgh = len;
			 * int index = Arrays.binarySearch(sortedBitCodes, bits); if (index
			 * > -1 && sortedBitLensByBitCode[index] == len) return
			 * sortedValuesByBitCode[index];
			 * 
			 * for (int j = i; sortedCodes[j + 1].bitLentgh == len && j <
			 * sortedCodes.length; j++) i++;
			 */

			{ // Variant 2:
				int index = bitCodeToValue[bits];
				if (index > -1 && sortedBitLensByBitCode[index] == len)
					return sortedValuesByBitCode[index];

				for (int j = i; sortedCodes[j + 1].bitLentgh == len
						&& j < sortedCodes.length; j++)
					i++;
			}

			/*
			 * Variant 3: for (int j = i; sortedCodes[j].bitLentgh == len && j <
			 * sortedCodes.length; j++) if (sortedCodes[j].bitCode == bits)
			 * return sortedCodes[j].value;
			 */
		}

		throw new RuntimeException("Not found.");
	}

	static Comparator<HuffmanBitCode> bitCodeComparator = new Comparator<HuffmanBitCode>() {

		@Override
		public int compare(HuffmanBitCode o1, HuffmanBitCode o2) {
			int result = o1.bitLentgh - o2.bitLentgh;
			if (result == 0)
				return o1.bitCode - o2.bitCode;
			else
				return result;
		}
	};
	static Comparator<HuffmanBitCode> valueComparator = new Comparator<HuffmanBitCode>() {

		@Override
		public int compare(HuffmanBitCode o1, HuffmanBitCode o2) {
			return o1.value - o2.value;
		}
	};

	private static int NumberOfSetBits(int i) {
		i = i - ((i >> 1) & 0x55555555);
		i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
		return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
	}

	public static void main(String[] args) throws IOException {
		int size = 10000000;

		long time5 = System.nanoTime();
		CompressionHeaderFactory.HuffmanParamsCalculator cal = new HuffmanParamsCalculator();
		cal.add(ReadTag.nameType3BytesToInt("OQ", 'Z'), size);
		cal.add(ReadTag.nameType3BytesToInt("X0", 'C'), size);
		cal.add(ReadTag.nameType3BytesToInt("X0", 'c'), size);
		cal.add(ReadTag.nameType3BytesToInt("X0", 's'), size);
		cal.add(ReadTag.nameType3BytesToInt("X1", 'C'), size);
		cal.add(ReadTag.nameType3BytesToInt("X1", 'c'), size);
		cal.add(ReadTag.nameType3BytesToInt("X1", 's'), size);
		cal.add(ReadTag.nameType3BytesToInt("XA", 'Z'), size);
		cal.add(ReadTag.nameType3BytesToInt("XC", 'c'), size);
		cal.add(ReadTag.nameType3BytesToInt("XT", 'A'), size);
		cal.add(ReadTag.nameType3BytesToInt("OP", 'i'), size);
		cal.add(ReadTag.nameType3BytesToInt("OC", 'Z'), size);
		cal.add(ReadTag.nameType3BytesToInt("BQ", 'Z'), size);
		cal.add(ReadTag.nameType3BytesToInt("AM", 'c'), size);

		cal.calculate();

		// CanonicalHuffmanIntegerCodec helper = new
		// CanonicalHuffmanIntegerCodec(
		// cal.values(), cal.bitLens());
		Helper helper = new Helper(cal.values(), cal.bitLens());
		long time6 = System.nanoTime();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(baos);

		long time1 = System.nanoTime();
		for (int i = 0; i < size; i++) {
			for (int b : cal.values()) {
				helper.write(bos, b);
			}
		}

		bos.close();
		long time2 = System.nanoTime();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DefaultBitInputStream bis = new DefaultBitInputStream(bais);

		long time3 = System.nanoTime();
		int counter = 0;
		for (int i = 0; i < size; i++) {
			for (int b : cal.values()) {
				int v = helper.read(bis);
				if (v != b)
					fail("Mismatch: " + v + " vs " + b + " at " + counter);

				counter++;
			}
		}
		long time4 = System.nanoTime();

		System.out
				.printf("Size: %d bytes, bits per value: %.2f, create time %dms, write time %d ms, read time %d ms.",
						baos.size(), 8f * baos.size() / size
								/ cal.values().length,
						(time6 - time5) / 1000000, (time2 - time1) / 1000000,
						(time4 - time3) / 1000000);

		// String message = "12341111111111111122222223334";
		//
		// ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// DefaultBitOutputStream bos = new DefaultBitOutputStream(baos);
		// HuffmanParamsCalculator c = new HuffmanParamsCalculator();
		// for (byte b : message.getBytes())
		// c.add(b);
		// c.calculate();
		//
		// CanonicalHuffmanIntegerCodec codec = new
		// CanonicalHuffmanIntegerCodec(
		// c.values(), c.bitLens());
		// for (byte b : message.getBytes())
		// codec.write(bos, new Integer(b));
		//
		// bos.close();
		//
		// ByteArrayInputStream bais = new
		// ByteArrayInputStream(baos.toByteArray());
		// DefaultBitInputStream bis = new DefaultBitInputStream(bais);
		// Helper helper = new Helper(c.values(), c.bitLens());
		//
		// for (byte b : message.getBytes()) {
		// int value = helper.read(bis);
		// if (b != value)
		// throw new RuntimeException(String.format(
		// "Expecting %d but got %d.", b, value));
		// }
		// System.out.println("Done.");
	}

}
