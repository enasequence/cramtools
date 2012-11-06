package net.sf.cram.huffman;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import net.sf.cram.EncodingParams;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.CanonicalHuffmanIntegerCodec;
import net.sf.cram.encoding.HuffmanIntegerEncoding;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.stats.CompressionHeaderFactory;
import net.sf.cram.stats.CompressionHeaderFactory.HuffmanParamsCalculator;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecordIterator;

import org.junit.Test;

public class TestCanonicalHuffmanByteCodec {

	@Test
	public void test() throws IOException {
		int[] values = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
		int[] lens = new int[] { 1, 2, 4, 4, 5, 5, 6, 6, 7, 7, 7, 8, 8 };
		CanonicalHuffmanIntegerCodec c = new CanonicalHuffmanIntegerCodec(values,
				lens);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(baos);
		for (int b : values) {
			c.write(bos, b);
		}

		bos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DefaultBitInputStream bis = new DefaultBitInputStream(bais);

		for (int b : values) {
			int v = c.read(bis);
			if (v != b)
				fail("Mismatch: " + v + " vs " + b);
		}
	}

	@Test
	public void test2() throws IOException {
		SAMFileReader r = new SAMFileReader(
				new File(
						"c:/temp/HG00096.mapped.illumina.mosaik.GBR.exome.20110411.chr20.bam"));
		SAMRecordIterator iterator = r.iterator();

		CompressionHeaderFactory.HuffmanParamsCalculator c = new HuffmanParamsCalculator();

		String[] names = new String [100000] ;
		for (int i = 0; i < names.length && iterator.hasNext(); i++) {
			names[i] = iterator.next().getReadName() ;
			c.add(names[i].length());
		}
		iterator.close();
		r.close();
		c.calculate();

		int[] values = c.values();
		int[] lens = c.bitLens();
		System.out.println(Arrays.toString(values));
		System.out.println(Arrays.toString(lens));

		EncodingParams params = HuffmanIntegerEncoding.toParam(values, lens);
		HuffmanIntegerEncoding e = new HuffmanIntegerEncoding();
		e.fromByteArray(params.params);

		BitCodec<Integer> codec = e.buildCodec(null, null);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(baos);
		for (int i = 0; i < names.length; i++) {
			codec.write(bos, names[i].length());
		}

		bos.close();

		codec = e.buildCodec(null, null);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DefaultBitInputStream bis = new DefaultBitInputStream(bais);

		for (int i = 0; i < names.length; i++) {
			int v = codec.read(bis);
			if (v != names[i].length())
				fail("Mismatch: " + v + " vs " + names[i].length());
		}
	}
}
