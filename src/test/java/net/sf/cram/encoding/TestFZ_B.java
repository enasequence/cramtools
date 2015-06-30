package net.sf.cram.encoding;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.build.CompressionHeaderFactory.EncodingLengthCalculator;
import net.sf.cram.build.CompressionHeaderFactory.IntegerEncodingCalculator;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.DefaultBitOutputStream;

public class TestFZ_B {

	public static void main(String[] args) throws IOException {
		File file = new File("c:/temp/FZ.B.hist");

		Scanner scanner = new Scanner(file);
		List<HistEntry> entries = new ArrayList<TestFZ_B.HistEntry>(2000);
		int count = 0;
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] chunks = line.split("\t");
			entries.add(new HistEntry(Integer.valueOf(chunks[0]), Integer.valueOf(chunks[1])));
			count += Integer.valueOf(chunks[1]);
		}
		scanner.close();

		IntegerEncodingCalculator c = new IntegerEncodingCalculator("qwe", 2000);
		for (int i = 2; i < 50; i++)
			c.calcs.add(new EncodingLengthCalculator(new GolombIntegerEncoding(i)));

		for (int i = 2; i < 50; i++)
			c.calcs.add(new EncodingLengthCalculator(new GolombRiceIntegerEncoding(i)));

		for (int i = 2; i < 50; i++) {
			c.calcs.add(new EncodingLengthCalculator(new SubexpIntegerEncoding(i)));
		}

		for (HistEntry e : entries) {
			for (int i = 0; i < e.count; i++)
				c.addValue(e.value);
		}
		Encoding<Integer> encoding = c.getBestEncoding();
		byte[] params = encoding.toByteArray();
		params = Arrays.copyOf(params, Math.min(params.length, 20));
		System.out.println(encoding.id().name() + Arrays.toString(params));

		long time1 = System.currentTimeMillis();
		BitCodec<Integer> codec = encoding.buildCodec(null, null);
		int bits = 0;
		for (HistEntry entry : entries) {
			bits += entry.count * codec.numberOfBits(entry.value);
		}
		System.out.printf("bits=%d, bits per value=%.2f\n", bits, ((float) bits) / count);

		short[] array = new short[entries.size() * count];
		int j = 0;
		for (HistEntry entry : entries) {
			for (int i = 0; i < entry.count; i++)
				array[j] = ((short) entry.value);
		}
		shuffleArray(array);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);
		for (Short value : array) {
			codec.write(bos, value.intValue());
		}

		ByteArrayOutputStream gBAOS = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(gBAOS);
		gos.write(baos.toByteArray());
		gos.close();

		System.out.printf("Gzipped bytes: " + gBAOS.size());

		long time2 = System.currentTimeMillis();
		System.out.printf("Time: %.2f\n", (time2 - time1) / 1000f);
	}

	static void shuffleArray(short[] ar) {
		Random rnd = new Random();
		for (int i = ar.length - 1; i >= 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			short a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

	private static class HistEntry {
		public int value;
		public int count;

		public HistEntry() {
		}

		public HistEntry(int value, int count) {
			this.value = value;
			this.count = count;
		}

	}
}
