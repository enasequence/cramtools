package net.sf.cram.ref;

import java.util.Arrays;

public class ReferenceRegion {
	public int index;
	public String name;
	public long alignmentStart, alignmentEnd;
	public int arrayStart, arraySpan;
	public byte[] array;

	/**
	 * @param bases
	 * @param sequenceIndex
	 * @param sequenceName
	 * @param alignmentStart
	 *            1-based inclusive
	 * @param alignmentEnd
	 *            1-based inclusive
	 */
	public ReferenceRegion(byte[] bases, int sequenceIndex,
			String sequenceName, long alignmentStart, long alignmentEnd) {
		this.array = bases;
		this.index = sequenceIndex;
		this.name = sequenceName;

		if (alignmentEnd == -1)
			alignmentEnd = bases.length;

		if (alignmentStart < 1 || alignmentEnd < alignmentStart
				|| alignmentEnd - alignmentStart > bases.length
				|| alignmentEnd - 1 > bases.length)
			throw new IllegalArgumentException(String.format(
					"Invalid reference region: %s, %d, %d.", sequenceName,
					alignmentStart, alignmentEnd));

		this.alignmentStart = alignmentStart;
		this.alignmentEnd = alignmentEnd;

		this.arrayStart = (int) (alignmentStart - 1);
		this.arraySpan = (int) (alignmentEnd - alignmentStart) + 1;
	}

	public int arrayPosition(long alignmentPosition) {
		int arrayPosition = (int) (arrayStart + (alignmentPosition - alignmentStart));

		if (arrayPosition < 0 || arrayPosition > array.length)
			throw new IllegalArgumentException(
					"The alignment position is out of the region: "
							+ alignmentPosition);

		return arrayPosition;
	}

	public byte base(long alignmentPosition) {
		return array[arrayPosition(alignmentPosition)];
	}

	public byte[] copy(long alignmentStart, int alignmentSpan) {
		int from = arrayPosition(alignmentStart);
		int to = arrayPosition(alignmentStart + alignmentSpan);
		return Arrays.copyOfRange(array, from, to);
	}
}