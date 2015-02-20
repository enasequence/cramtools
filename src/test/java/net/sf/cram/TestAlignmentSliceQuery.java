package net.sf.cram;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestAlignmentSliceQuery {

	@Test(expected = NullPointerException.class)
	public void constructorWithNull() {
		new AlignmentSliceQuery(null);
	}

	@Test(expected = NumberFormatException.class)
	public void constructorBadFormat() {
		new AlignmentSliceQuery("magna:carta-1215");
	}

	@Test(expected = NumberFormatException.class)
	public void constructorBadFormatTwo() {
		new AlignmentSliceQuery(":carta-1215");
	}

	@Test(expected = NumberFormatException.class)
	public void constructorBadFormatThree() {
		new AlignmentSliceQuery("a::3-4");
	}

	@Test(expected = NumberFormatException.class)
	public void constructorBadFormatFive() {
		new AlignmentSliceQuery("magna:1215z");
	}

	@Test
	public void constructorBasics() {
		AlignmentSliceQuery query = new AlignmentSliceQuery("");
		assertEquals("", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(0, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("nada");
		assertEquals("nada", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(0, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("magna:1215-6-5");
		assertEquals("magna", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(1215, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("magna:1215-666");
		assertEquals("magna", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(1215, query.start);
		assertEquals(666, query.end);

		// Happy path.
		query = new AlignmentSliceQuery("a:1-9");
		assertEquals("a", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(1, query.start);
		assertEquals(9, query.end);

		query = new AlignmentSliceQuery(":4-5");
		assertEquals("", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(4, query.start);
		assertEquals(5, query.end);

		query = new AlignmentSliceQuery("magna:1215");
		assertEquals("magna", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(1215, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("four:4-");
		assertEquals("four", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(4, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("five:5--");
		assertEquals("five", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(5, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("x:5:");
		assertEquals("x", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(5, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("x:5:6");
		assertEquals("x", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(5, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery("w:8--9");
		assertEquals("w", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(8, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);

		query = new AlignmentSliceQuery(":44-55");
		assertEquals("", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(44, query.start);
		assertEquals(55, query.end);

		query = new AlignmentSliceQuery("22-33");
		assertEquals("22-33", query.sequence);
		assertEquals(0, query.sequenceId);
		assertEquals(0, query.start);
		assertEquals(Integer.MAX_VALUE, query.end);
	}
}
