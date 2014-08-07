package net.sf.cram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import net.sf.cram.ref.ReferenceSource;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMIterator;
import net.sf.samtools.SAMRecord;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestReadingV3 {
	private String path;
	private InputStream is;
	private ReferenceSource referenceSource;

	public TestReadingV3(String path) {
		this.path = path;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] {
		// @formatter:off
		// { "data/v21.cram" },
		// { "data/v21_embeddedref.cram" }, { "data/v21_java.cram" },
		{ "data/v30_bz2.cram" },
		// { "data/v30_bz2_rans.cram" }, {
		// "data/v30_bz2_rans_lzma.cram" },
		// { "data/v30.cram" }, { "data/v30_embeddedref.cram" }, {
		// "data/v30_lzma.cram" },
		// { "data/v30_noref.cram" }, { "data/v30_rans.cram" },
		// @formatter:on
		};
		return Arrays.asList(data);
	}

	@Before
	public void before() throws FileNotFoundException {
		Log.setGlobalLogLevel(LogLevel.WARNING);
		is = getClass().getClassLoader().getResourceAsStream(path);
		if (is == null)
			throw new FileNotFoundException(path);
		referenceSource = new ReferenceSource(new File("src/test/resources/data/ref.fa"));
	}

	@Test
	public void testRead() throws IOException {
		try {
			SAMIterator si = new SAMIterator(is, referenceSource);
			int counter = 0;
			while (si.hasNext()) {
				SAMRecord record = si.next();
				assertNotNull(record);
				assertNotNull(record.getReadName());
				assertTrue(record.getReadLength() > 0);

				assertNotNull(record.getReadBases());
				assertEquals(String.format("Invalid base found: path %s, record %d, base count %d, read length %d.\n",
						path, counter, record.getReadBases().length, record.getReadLength()),
						record.getReadBases().length, record.getReadLength());
				byte[] bases = record.getReadBases();
				for (int position = 0; position < bases.length; position++)
					switch (bases[position]) {
					case 'A':
					case 'C':
					case 'G':
					case 'T':
					case 'N':
						break;

					default:
						fail(String.format("Invalid base found: path %s, record %d, position %d, found byte %d.\n",
								path, counter, position, bases[position]));
					}

				assertNotNull(record.getBaseQualities());
				assertEquals(record.getReadBases().length, record.getReadLength());

				byte[] scores = record.getBaseQualities();
				for (int position = 0; position < scores.length; position++)
					if (scores[position] > 39 || scores[position] < 0)
						fail(String.format(
								"Invalid quality score found: path %s, record %d, position %d, found byte %d.\n", path,
								counter, position, scores[position]));

			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
