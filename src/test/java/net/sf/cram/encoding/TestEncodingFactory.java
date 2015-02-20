package net.sf.cram.encoding;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.cram.structure.EncodingID;
import net.sf.cram.structure.EncodingParams;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEncodingFactory {

	private EncodingFactory factory;
	private static List<EncodingID> idsByteArrayEncoding;

	@BeforeClass
	public static void classSetUp() {
		// It would have been cool if the EncodingID enum
		// provided these values, but it doesn't (yet).
		idsByteArrayEncoding = new ArrayList<EncodingID>();
		idsByteArrayEncoding.add(EncodingID.BYTE_ARRAY_LEN);
		idsByteArrayEncoding.add(EncodingID.BYTE_ARRAY_STOP);
		idsByteArrayEncoding.add(EncodingID.EXTERNAL);
	}

	@Before
	public void setUp() {
		factory = new EncodingFactory(); // Class Under Test.
	}

	@Test(expected = NullPointerException.class)
	public void createEncodingWithNullDataSeriesType() {
		// Maybe a null guard should protect against a NPE?
		factory.createEncoding(null, EncodingID.EXTERNAL);
	}

	@Test(expected = NullPointerException.class)
	public void createEncodingWithNullEncodingID() {
		// Maybe a null guard should protect against a NPE here, too?
		factory.createEncoding(DataSeriesType.BYTE, null);
	}

	@Test
	public void createEncoding() {
		// This test is possibly going to be fragile.
		// Maybe the factory could provide valid values?

		// Creating our own valid values map instead of a
		// Junit Parameters values multi-dimensional array.
		Map<DataSeriesType, List<EncodingID>> validCombos =
				new HashMap<DataSeriesType, List<EncodingID>>();
		validCombos.put(DataSeriesType.BYTE,
				Arrays.asList(EncodingID.EXTERNAL,
						EncodingID.HUFFMAN,
						EncodingID.NULL));
		validCombos.put(DataSeriesType.INT,
				Arrays.asList(
						EncodingID.BETA,
						EncodingID.EXTERNAL,
						EncodingID.GAMMA,
						EncodingID.GOLOMB,
						EncodingID.GOLOMB_RICE,
						EncodingID.HUFFMAN,
						EncodingID.NULL,
						EncodingID.SUBEXP));
		validCombos.put(DataSeriesType.LONG,
				Arrays.asList(
						EncodingID.EXTERNAL,
						EncodingID.GOLOMB,
						EncodingID.NULL));
		validCombos.put(DataSeriesType.BYTE_ARRAY,
				Arrays.asList(EncodingID.BYTE_ARRAY_LEN,
						EncodingID.BYTE_ARRAY_STOP,
						EncodingID.EXTERNAL,
						EncodingID.NULL));

		for (DataSeriesType dst : validCombos.keySet()) {
			for (EncodingID eid : validCombos.get(dst)) {
				assertNotNull(factory.createEncoding(dst, eid));
			}
		}
	}

	@Test
	public void createEncodingToNullEncoding() {
		// A random selection to test.
		for (DataSeriesType dst : DataSeriesType.values()) {
			Encoding<?> e = factory.createEncoding(dst, EncodingID.NULL);
			assertTrue(e instanceof NullEncoding);
		}
	}

	@Test
	public void createEncodingToExternalNumberEncoding() {
		// Another random selection to test.
		for (DataSeriesType dst : DataSeriesType.values()) {
			Encoding<? extends Number> e =
					factory.createEncoding(dst, EncodingID.EXTERNAL);
			assertTrue(e instanceof Encoding<?>);
		}
	}

	@Test
	public void createByteArrayEncoding() {
		for (EncodingID id : EncodingID.values()) {
			if (idsByteArrayEncoding.contains(id)) {
				assertNotNull(factory.createByteArrayEncoding(id));
			} else {
				assertNull(factory.createByteArrayEncoding(id));
			}
		}
	}

	@Test
	public void createByteArrayEncodingTwo() {
		// Mostly from existing test in TestByteArrayLenEncoding.

		final EncodingID id = EncodingID.BYTE_ARRAY_LEN;

		// What we are testing.
		Encoding<byte[]> encoding = factory.createByteArrayEncoding(id);

		int[] values = new int[] { 1, 2, 3 };
		int[] bitLens = new int[] { 1, 2, 2 };
		EncodingParams lenParams =
				HuffmanIntegerEncoding.toParam(values, bitLens);

		EncodingParams byteParams =
				ExternalByteArrayEncoding.toParam(id.ordinal());

		EncodingParams ep = ByteArrayLenEncoding.toParam(lenParams, byteParams);
		encoding.fromByteArray(ep.params);
		ByteArrayLenEncoding bale = (ByteArrayLenEncoding) encoding;
		HuffmanIntegerEncoding hie = (HuffmanIntegerEncoding) bale.lenEncoding;

		assertArrayEquals(values, hie.values);
		assertArrayEquals(bitLens, hie.bitLengths);

		ExternalByteArrayEncoding ebae = (ExternalByteArrayEncoding) bale.byteEncoding;
		assertEquals(EncodingID.BYTE_ARRAY_LEN.ordinal(), ebae.contentId);
	}

	@Test
	public void createByteEncoding() {
		// For some reason, the only non-null
		// result is for EncodingID.EXTERNAL.
		for (EncodingID id : EncodingID.values()) {
			if (id != EncodingID.EXTERNAL) {
				assertNull(factory.createByteEncoding(id));
			}
		}
		assertNotNull(factory.createByteEncoding(EncodingID.EXTERNAL));
	}

	@Test
	public void createIntEncoding() {
		// Currently, all null results.
		for (EncodingID id : EncodingID.values()) {
			assertNull(factory.createIntEncoding(id));
		}
	}
}
