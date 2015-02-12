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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.io.IOUtils;

import org.junit.Ignore;
import org.junit.Test;

public class TestGolombRiceIntegerCodec {

	@Test
	public void test_numberOfBits() {
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(2);
		for (int value = 0; value < 1000; value++)
			assertThat(codec.numberOfBits(value), is(value / 4L + 3));
	}
	
	@Ignore("Used to print out values.")
	@Test
	public void printCodes_1_to_256() throws IOException {
		int golombRiceLogM = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(
				golombRiceLogM);
		for (int i = 0; i < 256; i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			BitOutputStream bos = new DefaultBitOutputStream(baos);
			int len = (int) codec.write(bos, i);
			bos.flush();

			byte[] buf = baos.toByteArray();

			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			BitInputStream bis = new DefaultBitInputStream(bais);
			long number = codec.read(bis);
			System.out.printf("%d: %d\t%s\t%d\t%s\n", i, number, IOUtils
					.toBitString(buf).subSequence(0, len), len, IOUtils
					.toBitString(buf));
		}
	}

	@Test
	public void test_0() throws IOException {
		int value = 0;
		long bitsLen = 3;
		int golombRiceLogM = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(
				golombRiceLogM);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);
		long len = codec.write(bos, value);
		bos.flush();

		assertThat(len, is(bitsLen));

		byte[] buf = baos.toByteArray();

		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		int number = codec.read(bis);
		assertThat(number, is(value));

		String bitsString = IOUtils.toBitString(buf);
		assertThat(bitsString, equalTo("10000000"));

		String cutBitsString = bitsString.substring(0, (int) len);
		assertThat(cutBitsString, equalTo("100"));
	}

	@Test
	public void test_1() throws IOException {
		int value = 1;
		long bitsLen = 3;
		int golombRiceLogM = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(
				golombRiceLogM);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);
		long len = codec.write(bos, value);
		bos.flush();

		assertThat(len, is(bitsLen));

		byte[] buf = baos.toByteArray();

		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		int number = codec.read(bis);
		assertThat(number, is(value));

		String bitsString = IOUtils.toBitString(buf);
		assertThat(bitsString, equalTo("10100000"));

		String cutBitsString = bitsString.substring(0, (int) len);
		assertThat(cutBitsString, equalTo("101"));
	}

	@Test
	public void test_20() throws IOException {
		int value = 20;
		long bitsLen = 8;
		int golombRiceLogM = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(
				golombRiceLogM);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);
		long len = codec.write(bos, value);
		bos.flush();

		assertThat(len, is(bitsLen));

		byte[] buf = baos.toByteArray();

		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		int number = codec.read(bis);
		assertThat(number, is(value));

		String bitsString = IOUtils.toBitString(buf);
		assertThat(bitsString, equalTo("00000100"));

		String cutBitsString = bitsString.substring(0, (int) len);
		assertThat(cutBitsString, equalTo("00000100"));
	}

	@Test
	public void test_255() throws IOException {
		int value = 255;
		long bitsLen = 66;
		int golombRiceLogM = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(
				golombRiceLogM);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);
		long len = codec.write(bos, value);
		bos.flush();

		assertThat(len, is(bitsLen));

		byte[] buf = baos.toByteArray();

		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		int number = codec.read(bis);
		assertThat(number, is(value));

		String bitsString = IOUtils.toBitString(buf);
		assertThat(
				bitsString,
				equalTo("000000000000000000000000000000000000000000000000000000000000000111000000"));

		String cutBitsString = bitsString.substring(0, (int) len);
		assertThat(
				cutBitsString,
				equalTo("000000000000000000000000000000000000000000000000000000000000000111"));
	}

	@Test(timeout = 300)
	public void becnmark_Write() throws IOException {
		int maxNumbers = 3000000;
		int log2m = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(log2m);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);

		for (int i = 0; i < maxNumbers; i++)
			codec.write(bos, 20);

		bos.flush();
		baos.close();
	}

	@Test(timeout = 200)
	public void becnmark_Read() throws IOException {
		int maxNumbers = 3000000;
		int log2m = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(log2m);
		byte oneByteValue = 20;

		byte[] buf = new byte[maxNumbers];
		Arrays.fill(buf, oneByteValue);
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		for (int i = 0; i < maxNumbers; i++)
			codec.read(bis);
	}

	@Test
	public void testRoundtrip() throws IOException {
		int maxValue = 10000;
		int log2m = 2;
		GolombRiceIntegerCodec codec = new GolombRiceIntegerCodec(log2m);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);

		for (int i = 0; i < maxValue; i++)
			codec.write(bos, i);

		bos.flush();
		baos.close();

		byte[] buf = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		BitInputStream bis = new DefaultBitInputStream(bais);
		for (int i = 0; i < maxValue; i++)
			assertThat(codec.read(bis), is(i));
	}
}
