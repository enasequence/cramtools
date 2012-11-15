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

		bases = new byte[Math.min(windowSize, reference.length)];
		coverage = new short[Math.min(windowSize, reference.length)];
		mismatches = new short[Math.min(windowSize, reference.length)];
		position = 1;

		for (int i = 0; i < reference.length; i++) {
			switch (reference[i]) {
			case 'A':
			case 'C':
			case 'G':
			case 'T':
			case 'N':
				break;
			case 'a':
				reference[i] = 'A';
				break;
			case 'c':
				reference[i] = 'C';
				break;
			case 'g':
				reference[i] = 'G';
				break;
			case 't':
				reference[i] = 'T';
				break;
			default:
				reference[i] = 'N';
				break;
			}
		}

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

	/**
	 * Shift the window forward to a new position on the reference.
	 * 
	 * @param newPos
	 *            1-based reference coordinate, must be greater than current
	 *            position and smaller than reference length.
	 */
	public void moveForwardTo(int newPos) {
		if (newPos - 1 >= reference.length)
			throw new RuntimeException("New position is beyond the reference.");

		if (newPos < position)
			throw new RuntimeException(
					"Cannot shift to smaller position on the reference.");

		if (newPos > reference.length - bases.length + 1)
			newPos = reference.length - bases.length + 1;

		if (newPos == position)
			return;

		System.arraycopy(reference, newPos - 1, bases, 0,
				Math.min(bases.length, reference.length - newPos + 1));

		if (newPos > position && position + bases.length - newPos > 0) {
			for (int i = 0; i < coverage.length; i++) {
				if (i + newPos - position < coverage.length) {
					coverage[i] = coverage[i + newPos - position];
					mismatches[i] = mismatches[i + newPos - position];
				} else {
					coverage[i] = 0;
					mismatches[i] = 0;
				}
			}
		} else {
			Arrays.fill(coverage, (short) 0);
			Arrays.fill(mismatches, (short) 0);
		}

		this.position = newPos;
	}

	public void reset() {
		System.arraycopy(reference, position - 1, bases, 0,
				Math.min(bases.length, reference.length - position + 1));
		Arrays.fill(coverage, (short) 0);
		Arrays.fill(mismatches, (short) 0);
	}

	public void ensureRange(int start, int length) {
		if (length > bases.length)
			throw new RuntimeException("Requested window is too big: " + length);
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
		if (pos - this.position >= coverage.length)
			return 0;
		else
			return coverage[pos - this.position];
	}

	public final short mismatchesAt(int pos) {
		if (pos - this.position >= coverage.length)
			return 0;
		else
			return mismatches[pos - this.position];
	}

	public final void addCoverage(int pos, int amount) {
		coverage[pos - this.position] += amount;
	}

	public final void addMismatches(int pos, int amount) {
		mismatches[pos - this.position] += amount;
	}
}