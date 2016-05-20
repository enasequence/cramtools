/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram.ref;

import htsjdk.samtools.util.SequenceUtil;

import java.util.Arrays;

public class ReferenceRegion {
	public int index;
	public String name;
	public long alignmentStart;
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
	public ReferenceRegion(byte[] bases, int sequenceIndex, String sequenceName, long alignmentStart) {
		this.array = bases;
		this.index = sequenceIndex;
		this.name = sequenceName;

		if (alignmentStart < 1)
			throw new IllegalArgumentException(String.format("Invalid reference region1: %s, %d, %d.", sequenceName,
					alignmentStart, bases.length));

		this.alignmentStart = alignmentStart;
	}

	public static ReferenceRegion wrap(byte[] bases, int sequenceIndex, String sequenceName) {
		return new ReferenceRegion(bases, sequenceIndex, sequenceName, 1);
	}

	public static ReferenceRegion copyRegion(byte[] bases, int sequenceIndex, String sequenceName, long alignmentStart,
			long alignmentEnd) {
		byte[] copy = new byte[(int) (alignmentEnd - alignmentStart + 1)];
		System.arraycopy(bases, (int) (alignmentStart - 1), copy, 0, copy.length);
		return new ReferenceRegion(copy, sequenceIndex, sequenceName, alignmentStart);
	}

	public int arrayPosition(long alignmentPosition) {
		int arrayPosition = (int) (alignmentPosition - alignmentStart);

		if (arrayPosition < 0 || arrayPosition > array.length - 1) {
			System.err.println(toString());
			throw new IllegalArgumentException(String.format(
					"The alignment position is out of the region: %d, %d, %d, %d", alignmentStart, alignmentPosition,
					0, arrayPosition));
		}

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

	public byte[] copySafe(long alignmentStart, int alignmentSpan) {
		int from = (int) (alignmentStart - this.alignmentStart);
		if (from > array.length - 1)
			return new byte[0];

		int to = (int) (alignmentStart + alignmentSpan - this.alignmentStart);

		if (to > array.length)
			to = array.length;
		return Arrays.copyOfRange(array, from, to);
	}

	public String md5(int alignmentStart, int alignmentSpan) {
		int from = (int) (alignmentStart - this.alignmentStart);
		if (from >= array.length)
			return SequenceUtil.calculateMD5String(new byte[0], 0, 0);
		// allow for hanging end:
		int to = (int) (alignmentStart + alignmentSpan - this.alignmentStart);

		return SequenceUtil.calculateMD5String(array, from, Math.min(to - from, array.length - from));
	}

	@Override
	public String toString() {
		return "ReferenceRegion [index=" + index + ", name=" + name + ", alignmentStart=" + alignmentStart
				+ ", array length=" + array.length + "]";
	}

}
