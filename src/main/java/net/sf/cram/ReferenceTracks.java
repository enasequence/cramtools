package net.sf.cram;

import java.util.Arrays;

public class ReferenceTracks {
	private int sequenceId;
	private String sequenceName;
	private byte[] reference;

	private int position;

	// a copy of ref bases for the given range:
	private final byte[] bases;
	private final short[] coverage;
	private final short[] mismatches;

	public ReferenceTracks(int sequenceId, String sequenceName,
			byte[] reference, int windowSize) {
		this.sequenceId = sequenceId;
		this.sequenceName = sequenceName;
		this.reference = reference;
		bases = new byte[windowSize];
		coverage = new short[windowSize];
		mismatches = new short[windowSize];
		position = 1;

		reset();
	}

	public int getSequenceId() {
		return sequenceId;
	}

	public String getSequenceName() {
		return sequenceName;
	}

	public int getWindowPosition() {
		return position;
	}

	public int getWindowLength() {
		return bases.length;
	}

	public int getReferenceLength() {
		return reference.length;
	}

	public void moveForwardTo(int newPos) {
		if (newPos < position)
			throw new RuntimeException(
					"Cannot shift to smaller position on the reference.");
		if (newPos == position)
			return;

		System.arraycopy(reference, newPos - 1, bases, 0, bases.length);

		if (newPos > position && position + bases.length - newPos > 0) {
			System.arraycopy(coverage, (newPos - position), coverage, 0,
					(position - newPos + coverage.length));
			System.arraycopy(mismatches, (newPos - position), mismatches,
					0, (position - newPos + coverage.length));
		} else {
			Arrays.fill(coverage, (short) 0);
			Arrays.fill(mismatches, (short) 0);
		}

		this.position = newPos;
	}

	public void reset() {
		System.arraycopy(reference, position - 1, bases, 0, bases.length);
		Arrays.fill(coverage, (short) 0);
		Arrays.fill(mismatches, (short) 0);
	}

	public void ensureRange(int start, int length) {
		if (length > bases.length)
			throw new RuntimeException("Requested window is too big: "
					+ length);
		if (start < position)
			throw new RuntimeException("Cannot move the window backwords: "
					+ start);

		if (start + length > position + bases.length)
			moveForwardTo(start);
	}

	public final byte baseAt(int pos) {
		return bases[pos - this.position];
	}

	public final short coverageAt(int pos) {
		return coverage[pos - this.position];
	}

	public final short mismatchesAt(int pos) {
		return mismatches[pos - this.position];
	}

	public final void addCoverage(int pos, int amount) {
		coverage[pos - this.position] += amount;
	}

	public final void addMismatches(int pos, int amount) {
		mismatches[pos - this.position] += amount;
	}
}