/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package net.sf.cram.fasta;

class FAIDX_FastaIndexEntry implements Comparable<FAIDX_FastaIndexEntry> {
	private String name;
	private int len;
	private long startPointer;
	private int lineWidthNoNL;
	private int lineWidthWithNL;
	private int index;

	public FAIDX_FastaIndexEntry(int index, String name, int len, long startPointer, int lineWidthNoNL,
			int lineWidthWithNL) {
		this.index = index;
		this.name = name;
		this.len = len;
		this.startPointer = startPointer;
		this.lineWidthNoNL = lineWidthNoNL;
		this.lineWidthWithNL = lineWidthWithNL;
	}

	@Override
	public String toString() {
		return String.format("%s\t%d\t%d\t%d\t%d", name, len, startPointer, lineWidthNoNL, lineWidthWithNL);
	}

	@Override
	public int compareTo(FAIDX_FastaIndexEntry o) {
		int result = name.compareTo(o.name);
		if (result != 0)
			return result;

		return (int) (startPointer - o.startPointer);
	}

	public static FAIDX_FastaIndexEntry fromString(int index, String line) {
		String[] values = line.split("\t");

		String name = values[0];

		int len = Integer.valueOf(values[1]);
		long startPointer = Long.valueOf(values[2]);
		int lineWidthNoNL = Integer.valueOf(values[3]);
		int lineWidthWithNL = Integer.valueOf(values[4]);

		return new FAIDX_FastaIndexEntry(index, name, len, startPointer, lineWidthNoNL, lineWidthWithNL);
	}

	public String getName() {
		return name;
	}

	public int getLen() {
		return len;
	}

	public long getStartPointer() {
		return startPointer;
	}

	public int getLineWidthNoNL() {
		return lineWidthNoNL;
	}

	public int getLineWidthWithNL() {
		return lineWidthWithNL;
	}

	public int getIndex() {
		return index;
	}

	public int getBasesPerLine() {
		return lineWidthNoNL;
	}

	public int getBytesPerLine() {
		return lineWidthWithNL;
	}

}