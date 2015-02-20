package net.sf.cram.cg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestEvidenceRecord {

	@Test
	public void defaults() {
		EvidenceRecord er = new EvidenceRecord();

		assertNull(er.IntervalId);
		assertNull(er.Chromosome);
		assertNull(er.Slide);
		assertNull(er.Lane);
		assertNull(er.FileNumInLane);
		assertNull(er.DnbOffsetInLaneFile);
		assertNull(er.AlleleIndex);
		assertNull(er.Side);
		assertNull(er.Strand);
		assertNull(er.OffsetInAllele);
		assertNull(er.AlleleAlignment);
		assertNull(er.OffsetInReference);
		assertNull(er.ReferenceAlignment);
		assertNull(er.MateOffsetInReference);
		assertNull(er.MateReferenceAlignment);
		assertNull(er.MappingQuality);
		assertNull(er.ScoreAllele0);
		assertNull(er.ScoreAllele1);
		assertNull(er.ScoreAllele2);
		assertNull(er.Sequence);
		assertNull(er.Scores);

		assertEquals(0, er.interval);
		assertEquals(0, er.side);
		assertFalse(er.negativeStrand);
		assertEquals(0, er.mapq);
		assertEquals(0, er.pos);

		assertNull(er.line);
		assertNull(er.name);
		assertNull(er.getReadName()); // A standard accessor.
		assertSame(er.name, er.getReadName());
	}

	@Test
	public void fromString() {
		final String line = "1\t"
				+ "two\t"
				+ "three\t"
				+ "four\t"
				+ "five\t"
				+ "six\t"
				+ "seven\t"
				+ "eight\t"
				+ "nine\t"
				+ "ten\t"
				+ "eleven\t"
				+ "12\t"
				+ "thirteen\t"
				+ "fourteen\t"
				+ "fifteen\t"
				+ "sixteen\t"
				+ "seventeen\t"
				+ "eighteen\t"
				+ "nineteen\t"
				+ "twenty\t"
				+ "twentyone";

		EvidenceRecord er = EvidenceRecord.fromString(line);
		assertEquals("1", er.IntervalId);
		assertEquals(1, er.interval);
		assertEquals("12", er.OffsetInReference);
		assertEquals(12, er.pos);

		assertEquals("1", er.IntervalId);
		assertEquals("two", er.Chromosome);
		assertEquals("three", er.Slide);
		assertEquals("four", er.Lane);
		assertEquals("five", er.FileNumInLane);
		assertEquals("six", er.DnbOffsetInLaneFile);
		assertEquals("seven", er.AlleleIndex);
		assertEquals("eight", er.Side);
		assertEquals("nine", er.Strand);
		assertEquals("ten", er.OffsetInAllele);
		assertEquals("eleven", er.AlleleAlignment);
		assertEquals("12", er.OffsetInReference);
		assertEquals("thirteen", er.ReferenceAlignment);
		assertEquals("fourteen", er.MateOffsetInReference);
		assertEquals("fifteen", er.MateReferenceAlignment);
		assertEquals("sixteen", er.MappingQuality);
		assertEquals("seventeen", er.ScoreAllele0);
		assertEquals("eighteen", er.ScoreAllele1);
		assertEquals("nineteen", er.ScoreAllele2);
		assertEquals("twenty", er.Sequence);
		assertEquals("twentyone", er.Scores);

		assertEquals(1, er.interval);
		assertEquals(1, er.side);
		assertTrue(er.negativeStrand);
		assertEquals(82, er.mapq);
		assertEquals(12, er.pos);

		assertEquals(line, er.line);
		assertEquals("three-four-five:six", er.name);
		assertSame(er.name, er.getReadName());
	}
}
