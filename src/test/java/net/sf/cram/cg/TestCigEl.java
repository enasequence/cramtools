package net.sf.cram.cg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestCigEl {

	@Test
	public void constructor() {
		CigEl cigEl = new CigEl();
		assertEquals(0, cigEl.len);
		assertEquals(0, cigEl.op);
		assertNull(cigEl.bases);
		assertNull(cigEl.scores);
	}

	@Test
	public void constructorWithArgs() {
		CigEl cigEl = new CigEl(7, 'C');
		assertEquals(7, cigEl.len);
		assertEquals('C', cigEl.op);
		assertNull(cigEl.bases);
		assertNull(cigEl.scores);
		assertTrue(cigEl.toString().contains("no bases"));
	}

	@Test
	public void byteBuffers() {
		CigEl cigEl = new CigEl(1, 'A');
		assertEquals(1, cigEl.len);
		assertEquals('A', cigEl.op);
		assertNull(cigEl.bases);
		assertNull(cigEl.scores);

		String s0 = "happy";
		byte[] bytes0 = s0.getBytes();
		cigEl.bases = ByteBuffer.allocate(bytes0.length + 8);
		cigEl.bases.put(bytes0);
		assertTrue(cigEl.toString().contains("happy"));
		assertTrue(cigEl.bases instanceof ByteBuffer);

		// Test the scores ByteBuffer, although
		// it appears to be usused anywhere else.
		String s1 = "dog";
		byte[] bytes1 = s1.getBytes();
		cigEl.scores = ByteBuffer.allocate(bytes1.length + 8);
		cigEl.scores.put(bytes1);
		assertFalse(cigEl.toString().contains("dog")); // Look.
	}
}
