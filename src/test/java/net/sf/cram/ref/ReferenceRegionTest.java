package net.sf.cram.ref;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import htsjdk.samtools.util.SequenceUtil;

public class ReferenceRegionTest {

	private byte[] data;

	@Before
	public void init() {
		data = new byte[1024];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) i;
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_Start_zero() {
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", 0);
		fail("Should fail due to alignment start less then 1");
	}

	@Test
	public void test_Start_1() {
		int start = 1;

		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);
		Assert.assertEquals(start, region.alignmentStart);
		Assert.assertEquals(0, region.arrayPosition(start));

		for (int pos = start, index = 0; pos < start + 10; pos++, index++) {
			Assert.assertEquals(data[index], region.base(pos));
		}

		int len = 10;
		String expectedMD5 = SequenceUtil.calculateMD5String(data, 0, len);
		String md5 = region.md5(start, len);
		Assert.assertEquals(expectedMD5, md5);
	}

	@Test
	public void test_Start_2() {
		int start = 2;

		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);
		Assert.assertEquals(start, region.alignmentStart);
		Assert.assertEquals(0, region.arrayPosition(start));

		for (int pos = start, index = 0; pos < start + 10; pos++, index++) {
			Assert.assertEquals(data[index], region.base(pos));
		}

		int len = 10;
		String expectedMD5 = SequenceUtil.calculateMD5String(data, 0, len);
		String md5 = region.md5(start, len);
		Assert.assertEquals(expectedMD5, md5);
	}

	@Test
	public void test_Start_AtEnd() {
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", 1);
		int start = data.length;

		Assert.assertEquals(1, region.alignmentStart);
		Assert.assertEquals(data.length - 1, region.arrayPosition(start));
		Assert.assertEquals(data[data.length - 1], region.base(start));
	}

	@Test
	public void test_HangingEnd() {
		int start = 2;

		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);
		Assert.assertEquals(start, region.alignmentStart);
		Assert.assertEquals(0, region.arrayPosition(start));

		for (int pos = start, index = 0; pos < start + 10; pos++, index++) {
			Assert.assertEquals(data[index], region.base(pos));
		}

		String expectedMD5 = SequenceUtil.calculateMD5String(data, 0, data.length);
		String md5 = region.md5(start, data.length + 10);
		Assert.assertEquals(expectedMD5, md5);

		md5 = region.md5(start, data.length + 100);
		Assert.assertEquals(expectedMD5, md5);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_HangingStart() {
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", 1);
		int start = data.length;
		Assert.assertEquals(data.length - 1, region.arrayPosition(start));

		start = data.length + 1;
		Assert.assertEquals(data.length, region.arrayPosition(start));
		region.arrayPosition(start);
		fail("Should fail due to start out of array");
	}

	@Test
	public void test_HangingStartMD5() {

		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", 1);
		int start = data.length + 1;

		String expectedMD5 = SequenceUtil.calculateMD5String("".getBytes(), 0, 0);
		String md5 = region.md5(start, 10);
		Assert.assertEquals(expectedMD5, md5);
	}

	@Test
	public void test_Copy_1() {
		int start = 1;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		byte[] copy = region.copy(start, len);
		for (int pos = start, index = 0; pos < len + 1; pos++, index++) {
			Assert.assertEquals(region.array[index], copy[index]);
		}
	}

	@Test
	public void test_Copy_2() {
		int start = 1;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		byte[] copy = region.copy(start + 1, len);
		Assert.assertEquals(len, copy.length);
		for (int pos = start + 1, index = 1; pos < len + 1; pos++, index++) {
			Assert.assertEquals(region.array[index], copy[index - 1]);
		}
	}

	@Test
	public void test_Copy_3() {
		int start = 3;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		int copyAlignmentStart = start + 13;
		byte[] copy = region.copy(copyAlignmentStart, len);
		Assert.assertEquals(len, copy.length);
		for (int index = 0; index < len; index++) {
			Assert.assertEquals(region.array[index + (copyAlignmentStart - start)], copy[index]);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_Copy_Beyond_Fails() {
		int start = 1;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		int copyAlignmentStart = start + data.length - 1;
		byte[] copy = region.copy(copyAlignmentStart, len);
		fail("Should fail due to end beyond array");
	}

	@Test
	public void test_CopySafe_HangingEnd_returns_clipped() {
		int start = 1;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		int copyAlignmentStart = start + data.length - 1;
		byte[] copy = region.copySafe(copyAlignmentStart, len);
		Assert.assertEquals(1, copy.length);
		for (int index = 0; index < copy.length; index++) {
			Assert.assertEquals(region.array[index + (copyAlignmentStart - start)], copy[index]);
		}
	}

	@Test
	public void test_CopySafe_Beyond_returns_empty() {
		int start = 1;
		ReferenceRegion region = new ReferenceRegion(data, 0, "chr1", start);

		int len = 100;
		int copyAlignmentStart = start + data.length;
		byte[] copy = region.copySafe(copyAlignmentStart, len);
		Assert.assertEquals(0, copy.length);
	}

	@Test
	public void test_copyRegion() {
		ReferenceRegion region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 1, 4);
		assertArrayEquals("ACGT".getBytes(), region.array);
		assertEquals(1, region.alignmentStart);

		region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 2, 4);
		assertArrayEquals("CGT".getBytes(), region.array);
		assertEquals(2, region.alignmentStart);

		region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 1, 5);
		assertArrayEquals("ACGT".getBytes(), region.array);
		assertEquals(1, region.alignmentStart);

		region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 4, 5);
		assertArrayEquals("T".getBytes(), region.array);
		assertEquals(4, region.alignmentStart);

		region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 4, 6);
		assertArrayEquals("T".getBytes(), region.array);
		assertEquals(4, region.alignmentStart);

		region = ReferenceRegion.copyRegion("ACGT".getBytes(), 0, "seq1", 5, 10);
		assertArrayEquals("".getBytes(), region.array);
		assertEquals(5, region.alignmentStart);
	}

}
