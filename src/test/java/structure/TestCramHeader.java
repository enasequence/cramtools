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
