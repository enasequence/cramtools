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
package net.sf.cram.huffman;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.sf.cram.build.CompressionHeaderFactory;
import net.sf.cram.build.CompressionHeaderFactory.HuffmanParamsCalculator;
import net.sf.cram.encoding.CanonicalHuffmanIntegerCodec;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.structure.ReadTag;

import org.junit.Test;

public class TestCanonicalHuffmanIntCodec {

	@Test
	public void test() throws IOException {
		int size = 100000 ;
		
		long time5 = System.nanoTime() ;
		CompressionHeaderFactory.HuffmanParamsCalculator cal = new HuffmanParamsCalculator();
		for (int i = 0; i < size; i++) {
			cal.add(ReadTag.nameType3BytesToInt("OQ", 'Z'));
			cal.add(ReadTag.nameType3BytesToInt("X0", 'C'));
			cal.add(ReadTag.nameType3BytesToInt("X0", 'c'));
			cal.add(ReadTag.nameType3BytesToInt("X0", 's'));
			cal.add(ReadTag.nameType3BytesToInt("X1", 'C'));
			cal.add(ReadTag.nameType3BytesToInt("X1", 'c'));
			cal.add(ReadTag.nameType3BytesToInt("X1", 's'));
			cal.add(ReadTag.nameType3BytesToInt("XA", 'Z'));
			cal.add(ReadTag.nameType3BytesToInt("XC", 'c'));
			cal.add(ReadTag.nameType3BytesToInt("XT", 'A'));
			cal.add(ReadTag.nameType3BytesToInt("OP", 'i'));
			cal.add(ReadTag.nameType3BytesToInt("OC", 'Z'));
			cal.add(ReadTag.nameType3BytesToInt("BQ", 'Z'));
			cal.add(ReadTag.nameType3BytesToInt("AM", 'c'));
		}

		cal.calculate();

		CanonicalHuffmanIntegerCodec c = new CanonicalHuffmanIntegerCodec(
				cal.values(), cal.bitLens());
		long time6 = System.nanoTime() ;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(baos);
		
		long time1=System.nanoTime() ;
		for (int i = 0; i < size; i++) {
			for (int b : cal.values()) {
				c.write(bos, b);
			}
		}

		bos.close();
		long time2=System.nanoTime() ;

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DefaultBitInputStream bis = new DefaultBitInputStream(bais);

		long time3=System.nanoTime() ;
		for (int i = 0; i < size; i++) {
			for (int b : cal.values()) {
				int v = c.read(bis);
				if (v != b)
					fail("Mismatch: " + v + " vs " + b);
			}
		}
		long time4=System.nanoTime() ;
		
		System.out.printf("Size: %d bytes, bits per value: %.2f, create time %dms, write time %d ms, read time %d ms.", baos.size(), 8f*baos.size()/size/cal.values().length, (time6-time5)/1000000, (time2-time1)/1000000, (time4-time3)/1000000);
	}
}
