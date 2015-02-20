package net.sf.cram.cg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestSplitter {

	@Test(expected = NullPointerException.class)
	public void packWithNullList() {
		// Perhaps the method should include a
		// null guard instead of throwing a NPE?
		Splitter.pack(null);
	}

	@Test
	public void packWithEmptyList() {
		List<CigEl> list = new ArrayList<CigEl>(0);
		Splitter.pack(list);
		assertTrue(list.isEmpty());
	}

	@Test
	public void packSimple() {
		List<CigEl> list = new ArrayList<CigEl>();
		Splitter.pack(list);
		assertTrue(list.isEmpty());

		final CigEl ce0 = new CigEl(1, 'A'); // For reference.
		CigEl ce1 = new CigEl(1, 'A');
		CigEl ce2 = new CigEl(1, 'A');
		assertTrue(equals(ce0, ce1));
		assertTrue(equals(ce0, ce2));

		list.add(ce1);
		list.add(ce2);

		List<CigEl> results = Splitter.pack(list);

		// Original list not changed.
		assertFalse(list.isEmpty());
		assertEquals(2, list.size());
		assertTrue(equals(ce0, ce1));
		assertTrue(equals(ce0, ce2));
		assertTrue(equals(ce0, list.get(0)));
		assertTrue(equals(ce0, list.get(1)));

		// Results list, shorter and changed.
		assertFalse(results.isEmpty());
		assertEquals(1, results.size()); // Look.
		assertFalse(equals(ce0, results.get(0))); // Look.
		assertEquals(ce0.len + 1, results.get(0).len); // Look.
		assertEquals(ce0.op, results.get(0).op); // Still 'A'
	}

	@Test
	public void packSimpleTwo() {
		List<CigEl> list = new ArrayList<CigEl>();
		Splitter.pack(list);
		assertTrue(list.isEmpty());

		final CigEl ce0 = new CigEl(1, 'A'); // For reference.
		CigEl ce1 = new CigEl(1, 'A');
		CigEl ce2 = new CigEl(1, 'A');
		CigEl ce3 = new CigEl(1, 'A');
		assertTrue(equals(ce0, ce1));
		assertTrue(equals(ce0, ce2));
		assertTrue(equals(ce0, ce3));

		list.add(ce1);
		list.add(ce2);
		list.add(ce3);

		List<CigEl> results = Splitter.pack(list);

		// Original list not changed.
		assertFalse(list.isEmpty());
		assertEquals(3, list.size());
		assertTrue(equals(ce0, ce1));
		assertTrue(equals(ce0, ce2));
		assertTrue(equals(ce0, list.get(0)));
		assertTrue(equals(ce0, list.get(1)));
		assertTrue(equals(ce0, list.get(2)));

		// Results list, shorter and changed.
		assertFalse(results.isEmpty());
		assertEquals(1, results.size()); // Look.
		assertFalse(equals(ce0, results.get(0))); // Look.
		assertEquals(ce0.len + 2, results.get(0).len); // Look.
		assertEquals(ce0.op, results.get(0).op); // Still 'A'
	}

	@Test
	public void packSimpleThree() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 100; i++) {
			list.add(new CigEl(1, 'A'));
		}
		assertEquals(100, list.size());

		List<CigEl> results = Splitter.pack(list);
		assertEquals(1, results.size()); // Note result.
		assertEquals('A', results.get(0).op);
		assertEquals(100, results.get(0).len); // Note.
	}

	@Test
	public void packSimpleFour() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 100; i++) {
			list.add(new CigEl(1, 'A'));
			list.add(new CigEl(2, 'B'));
			list.add(new CigEl(3, 'C'));
		}
		assertEquals(3 * 100, list.size());

		List<CigEl> results = Splitter.pack(list);
		assertEquals(3 * 100, results.size());

		for (int i = 0; i < 100; i = i + 3) {
			assertEquals('A', results.get(i).op);
			assertEquals(1, results.get(i).len);

			assertEquals('B', results.get(i + 1).op);
			assertEquals(2, results.get(i + 1).len);

			assertEquals('C', results.get(i + 2).op);
			assertEquals(3, results.get(i + 2).len);
		}
	}

	@Test
	public void packCollapsibleTypeZero() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
		}
		assertEquals(20, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(10, results.size());

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			Integer count = new Integer(opMap.get(opCode) + 1);
			opMap.put(opCode, count);
		}

		// Note 'D' op code counts now.
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeOne() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
			list.add(new CigEl(1, 'P'));
		}
		assertEquals(30, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(20, results.size());

		Map<Integer, Integer> opMap = createOpCountMap();
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('X')).intValue());

		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			Integer count = new Integer(opMap.get(opCode) + 1);
			opMap.put(opCode, count);
		}

		// Note 'D' op code counts now.
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(9, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(1, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(9, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(1, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeTwo() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
			list.add(new CigEl(1, 'M'));
			list.add(new CigEl(1, 'P'));
		}
		assertEquals(40, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(29, results.size()); // Look.

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			opMap.put(opCode, new Integer(opMap.get(opCode) + 1));
		}

		// Note op code counts now.
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(9, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(9, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(1, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeThree() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
			list.add(new CigEl(1, 'M'));
			list.add(new CigEl(1, 'N'));
			list.add(new CigEl(1, 'P'));
		}
		assertEquals(50, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(39, results.size()); // Look.

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			opMap.put(opCode, new Integer(opMap.get(opCode) + 1));
		}

		// Note op code counts now.
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(9, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(19, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(1, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeFour() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
			list.add(new CigEl(1, 'M'));
			list.add(new CigEl(1, 'N'));
			list.add(new CigEl(1, 'P'));
			list.add(new CigEl(1, 'X'));
		}
		assertEquals(60, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(40, results.size()); // Look.

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			opMap.put(opCode, new Integer(opMap.get(opCode) + 1));
		}

		// Note op code counts now.
		assertEquals(0, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeFive() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'B'));
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(1, 'I'));
			list.add(new CigEl(1, 'M'));
			list.add(new CigEl(1, 'N'));
			list.add(new CigEl(1, 'P'));
			list.add(new CigEl(1, 'X'));
		}
		assertEquals(70, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(50, results.size()); // Look.

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			opMap.put(opCode, new Integer(opMap.get(opCode) + 1));
		}

		// Note op code counts now.
		assertEquals(10, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('X')).intValue());
	}

	@Test
	public void packCollapsibleTypeSix() {
		List<CigEl> list = new ArrayList<CigEl>();
		for (int i = 0; i < 10; i++) {
			list.add(new CigEl(1, 'B'));
			list.add(new CigEl(1, 'D'));
			list.add(new CigEl(2, 'I')); // <-- Look.
			list.add(new CigEl(1, 'M'));
			list.add(new CigEl(1, 'N'));
			list.add(new CigEl(1, 'P'));
			list.add(new CigEl(1, 'X'));
		}
		assertEquals(70, list.size());

		// What we are testing.
		List<CigEl> results = Splitter.pack(list);

		assertEquals(60, results.size()); // Look.

		Map<Integer, Integer> opMap = createOpCountMap();
		for (CigEl ce : results) {
			Integer opCode = Integer.valueOf(ce.op);
			opMap.put(opCode, new Integer(opMap.get(opCode) + 1));
		}

		// Note op code counts now, especially 'D' and 'I'.
		assertEquals(10, opMap.get(Integer.valueOf('B')).intValue());
		assertEquals(0, opMap.get(Integer.valueOf('D')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('I')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('M')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('N')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('P')).intValue());
		assertEquals(10, opMap.get(Integer.valueOf('X')).intValue());
	}

	// Helper method needed since CigEl does
	// not (yet) have an equals method.
	private boolean equals(CigEl ce1, CigEl ce2) {
		if (ce1.bases == null) {
			if (ce2.bases != null)
				return false;
		} else if (!ce1.bases.equals(ce2.bases))
			return false;
		if (ce1.len != ce2.len)
			return false;
		if (ce1.op != ce2.op)
			return false;
		if (ce1.scores == null) {
			if (ce2.scores != null)
				return false;
		} else if (!ce1.scores.equals(ce2.scores))
			return false;
		return true;
	}

	private Map<Integer, Integer> createOpCountMap() {
		Map<Integer, Integer> opMap = new HashMap<Integer, Integer>();
		opMap.put(Integer.valueOf('B'), new Integer(0));
		opMap.put(Integer.valueOf('D'), new Integer(0));
		opMap.put(Integer.valueOf('I'), new Integer(0));
		opMap.put(Integer.valueOf('M'), new Integer(0));
		opMap.put(Integer.valueOf('N'), new Integer(0));
		opMap.put(Integer.valueOf('P'), new Integer(0));
		opMap.put(Integer.valueOf('X'), new Integer(0));

		return opMap;
	}
}
