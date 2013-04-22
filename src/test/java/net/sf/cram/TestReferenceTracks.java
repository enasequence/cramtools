package net.sf.cram;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import net.sf.cram.ref.ReferenceTracks;

import org.junit.Test;

public class TestReferenceTracks {

	@Test
	public void test6() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, 2);

		t.addCoverage(1, 1);
		t.addCoverage(2, 2);

		assertThat(t.coverageAt(1), is((short) 1));
		assertThat(t.coverageAt(2), is((short) 2));

		t.moveForwardTo(2);

		assertThat(t.coverageAt(2), is((short) 2));
		t.addCoverage(3, 3);
		assertThat(t.coverageAt(3), is((short) 3));
		
		t.moveForwardTo(3);
		
		assertThat(t.coverageAt(3), is((short) 3));
		t.addCoverage(4, 4);
		assertThat(t.coverageAt(4), is((short) 4));
		
		t.moveForwardTo(4);

		assertThat(t.coverageAt(4), is((short) 4));
		t.addCoverage(5, 5);
		assertThat(t.coverageAt(5), is((short) 5));

		t.moveForwardTo(5);

		assertThat(t.coverageAt(5), is((short) 5));
	}

	@Test
	public void test1() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, 2);

		assertThat(t.baseAt(1), is(ref[0]));
		assertThat(t.baseAt(2), is(ref[1]));

		t.moveForwardTo(2);

		assertThat(t.baseAt(2), is(ref[1]));
		assertThat(t.baseAt(3), is(ref[2]));

		t.moveForwardTo(3);

		assertThat(t.baseAt(3), is(ref[2]));
		assertThat(t.baseAt(4), is(ref[3]));

		t.moveForwardTo(4);

		assertThat(t.baseAt(4), is(ref[3]));
		assertThat(t.baseAt(5), is(ref[4]));

		t.moveForwardTo(5);

		assertThat(t.baseAt(5), is(ref[4]));
	}

	@Test
	public void test2() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, 2);

		t.moveForwardTo(5);

		assertThat(t.baseAt(5), is(ref[4]));
	}

	@Test(expected = Exception.class)
	public void test3() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, 2);

		t.moveForwardTo(ref.length + 1);
	}

	@Test(expected = Exception.class)
	public void test4() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, 2);

		t.moveForwardTo(0);
	}

	@Test
	public void test5() {
		byte[] ref = "12345".getBytes();

		ReferenceTracks t = new ReferenceTracks(1, "seq1", ref, ref.length + 5);

		assertThat(t.baseAt(1), is(ref[0]));
		assertThat(t.baseAt(2), is(ref[1]));

		t.moveForwardTo(2);

		assertThat(t.baseAt(2), is(ref[1]));
		assertThat(t.baseAt(3), is(ref[2]));

		t.moveForwardTo(3);

		assertThat(t.baseAt(3), is(ref[2]));
		assertThat(t.baseAt(4), is(ref[3]));

		t.moveForwardTo(4);

		assertThat(t.baseAt(4), is(ref[3]));
		assertThat(t.baseAt(5), is(ref[4]));

		t.moveForwardTo(5);

		assertThat(t.baseAt(5), is(ref[4]));
	}

}
