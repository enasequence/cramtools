package net.sf.cram.cg;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;

import org.junit.Test;

public class TestUtils {

	@Test(expected = NullPointerException.class)
	public void nullCigarArg() {
		// Maybe this should return an empty list
		// instead of throwing an exception?
		Utils.parseCigarInto(null, new ArrayList<CigEl>());
	}

	@Test(expected = NullPointerException.class)
	public void nullListArg() {
		// Maybe this should return an empty list
		// instead of throwing an exception?
		Utils.parseCigarInto("a", null);
	}

	@Test
	public void emptyCigarArg() {
		// Maybe this should return an
		// empty list instead of a null?
		List<CigEl> results = Utils.parseCigarInto("", null);
		assertNull(results);
	}

	@Test
	public void parseCigarIntoWithEmptyCigar() {
		String cigar = ""; // Empty.
		List<CigEl> list = createCigElList(100);
		assertNotNull(list);
		List<CigEl> results = Utils.parseCigarInto(cigar, list);
		assertEquals(100, results.size());
		assertEquals(100, list.size());
		assertTrue(list.containsAll(results));
		assertArrayEquals(list.toArray(), results.toArray());
	}

	@Test
	public void parseCigarIntoWithCigar() {
		String cigar = "abcdefgh";
		List<CigEl> list = createCigElList(0);
		assertNotNull(list);
		List<CigEl> results = Utils.parseCigarInto(cigar, list);
		assertEquals(8, results.size());
		assertArrayEquals(list.toArray(), results.toArray());

		for (int i = 0; i < cigar.length(); i++) {
			char ch = cigar.charAt(i);
			assertEquals(ch, results.get(i).op);
			assertEquals(0, results.get(i).len);
		}
	}

	@Test
	public void parseCigarIntoWithCigarTwo() {
		String cigar = "01234567";
		List<CigEl> list = createCigElList(0);
		List<CigEl> results = Utils.parseCigarInto(cigar, list);

		// Strange result, methinks.
		assertEquals(0, results.size());
		assertArrayEquals(list.toArray(), results.toArray());
	}

	@Test
	public void parseCigarIntoWithCigarThree() {
		String cigar = "01234567a";
		List<CigEl> list = createCigElList(0);
		List<CigEl> results = Utils.parseCigarInto(cigar, list);

		assertEquals(1, results.size());
		assertArrayEquals(list.toArray(), results.toArray());
		CigEl cigEl = results.get(0);
		assertEquals('a', cigEl.op);
		assertEquals(1234567, cigEl.len);
	}

	@Test
	public void parseCigarIntoWithCigarFour() {
		String cigar = "123a456b7c";
		List<CigEl> list = createCigElList(0);
		List<CigEl> results = Utils.parseCigarInto(cigar, list);

		assertEquals(3, results.size());
		assertArrayEquals(list.toArray(), results.toArray());
		CigEl cigEl = results.get(0);
		assertEquals('a', cigEl.op);
		assertEquals(123, cigEl.len);
		cigEl = results.get(1);
		assertEquals('b', cigEl.op);
		assertEquals(456, cigEl.len);
		cigEl = results.get(2);
		assertEquals('c', cigEl.op);
		assertEquals(7, cigEl.len);

		// Add more.
		cigar = "8d99e100f";
		results = Utils.parseCigarInto(cigar, results);
		assertEquals(6, results.size()); // Three more.
		cigEl = results.get(0);
		assertEquals('a', cigEl.op);
		assertEquals(123, cigEl.len);
		cigEl = results.get(1);
		assertEquals('b', cigEl.op);
		assertEquals(456, cigEl.len);
		cigEl = results.get(2);
		assertEquals('c', cigEl.op);
		assertEquals(7, cigEl.len);
		cigEl = results.get(3);
		assertEquals('d', cigEl.op);
		assertEquals(8, cigEl.len);
		cigEl = results.get(4);
		assertEquals('e', cigEl.op);
		assertEquals(99, cigEl.len);
		cigEl = results.get(5);
		assertEquals('f', cigEl.op);
		assertEquals(100, cigEl.len);
	}

	@Test
	public void toCigarOperatorList() {
		String cigar = "123I456D7S";
		List<CigEl> list = Utils.parseCigarInto(cigar, new ArrayList<CigEl>());
		assertEquals(3, list.size());

		List<CigarElement> results = Utils.toCigarOperatorList(list);
		assertEquals(3, results.size());
		CigarElement cigEl = results.get(0);
		assertEquals(CigarOperator.INSERTION, cigEl.getOperator());
		assertEquals(123, cigEl.getLength());
		cigEl = results.get(1);
		assertEquals(CigarOperator.DELETION, cigEl.getOperator());
		assertEquals(456, cigEl.getLength());
		cigEl = results.get(2);
		assertEquals(CigarOperator.SOFT_CLIP, cigEl.getOperator());
		assertEquals(7, cigEl.getLength());
	}

	@Test
	public void toByteArray() {
		CigEl cigEl = new CigEl(1, 'A');
		String s0 = "happy";
		byte[] bytes0 = s0.getBytes();
		cigEl.bases = ByteBuffer.allocate(bytes0.length);
		cigEl.bases.put(bytes0);
		assertTrue(cigEl.toString().contains("happy"));
		assertTrue(cigEl.bases instanceof ByteBuffer);

		byte[] byteArray = Utils.toByteArray(cigEl.bases);
		assertTrue(byteArray instanceof byte[]);
		assertEquals(s0.length(), byteArray.length);

		for (int i = 0; i < s0.length(); i++) {
			assertEquals(s0.charAt(i), byteArray[i]);
		}
	}

	@Test
	public void slice() {
		String s0 = "happy";
		byte[] bytes0 = s0.getBytes();
		ByteBuffer bb0 = ByteBuffer.allocate(bytes0.length);
		bb0.put(bytes0);
		assertEquals(s0.length(), bb0.capacity());
		assertEquals(s0.length(), bb0.position());
		assertEquals(s0.length(), bb0.limit());

		ByteBuffer bb1 = Utils.slice(bb0, 3, 5);

		assertEquals(2, bb1.capacity());
		assertEquals(0, bb1.position());
		assertEquals(2, bb1.limit());
	}

	private List<CigEl> createCigElList(int size) {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < size; i++) {
			list.add(new CigEl());

		}
		return list;
	}

}
