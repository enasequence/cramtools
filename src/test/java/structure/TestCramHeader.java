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
package structure;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import net.sf.cram.build.CramIO;
import net.sf.cram.structure.CramHeader;

import org.junit.Assert;
import org.junit.Test;

public class TestCramHeader {

	@Test
	public void test() throws IOException {

		String cramPath = "/data/set1/small.cram";
		InputStream stream = getClass().getResourceAsStream(
				cramPath);

		if (stream == null)
			fail("CRAM file not found: " + cramPath);

		CramHeader cramHeader = CramIO.readCramHeader(stream);

		assertNotNull(cramHeader);
		assertEquals(cramHeader.majorVersion, 1) ;
		assertEquals(cramHeader.minorVersion, 1) ;
		assertNotNull(cramHeader.samFileHeader) ;
	}

}
