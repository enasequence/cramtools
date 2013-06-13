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
package net.sf.cram.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;

public class CramIndex {
	private OutputStream os;

	public CramIndex(OutputStream os) {
		this.os = os;
	}

	public void addContainer(Container c) throws IOException {
		for (int i = 0; i < c.slices.length; i++) {
			Slice s = c.slices[i];
			Entry e = new Entry();
			e.sequenceId = c.sequenceId;
			e.alignmentStart = s.alignmentStart;
			e.alignmentSpan = s.alignmentSpan;
			e.containerStartOffset = c.offset;
			e.sliceOffset = c.landmarks[i];
			e.sliceSize = s.size;

			e.sliceIndex = i;

			String string = e.toString();
			os.write(string.getBytes());
			os.write('\n');
		}
	}

	public static class Entry implements Comparable<Entry>, Cloneable {
		public int sequenceId;
		public int alignmentStart;
		public int alignmentSpan;
		public long containerStartOffset;
		public int sliceOffset;
		public int sliceSize;
		public int sliceIndex;

		public Entry() {
		}

		public Entry(String line) {
			String[] chunks = line.split("\t");
			if (chunks.length != 6)
				throw new RuntimeException("Invalid index format.");

			sequenceId = Integer.valueOf(chunks[0]);
			alignmentStart = Integer.valueOf(chunks[1]);
			alignmentSpan = Integer.valueOf(chunks[2]);
			containerStartOffset = Long.valueOf(chunks[3]);
			sliceOffset = Integer.valueOf(chunks[4]);
			sliceSize = Integer.valueOf(chunks[5]);
		}

		@Override
		public String toString() {
			return String.format("%d\t%d\t%d\t%d\t%d\t%d", sequenceId,
					alignmentStart, alignmentSpan, containerStartOffset,
					sliceOffset, sliceSize);
		}

		@Override
		public int compareTo(Entry o) {
			if (sequenceId != o.sequenceId)
				return sequenceId - o.sequenceId;
			if (alignmentStart != o.alignmentStart)
				return alignmentStart - o.alignmentStart;
			return 0;
		}

		@Override
		public Entry clone() throws CloneNotSupportedException {
			Entry entry = new Entry();
			entry.sequenceId = sequenceId;
			entry.alignmentStart = alignmentStart;
			entry.alignmentSpan = alignmentSpan;
			entry.containerStartOffset = containerStartOffset;
			entry.sliceOffset = sliceOffset;
			entry.sliceSize = sliceSize;
			return entry;
		}
	}

	public static List<Entry> readIndex(InputStream is) {
		List<Entry> list = new LinkedList<CramIndex.Entry>();
		Scanner scanner = new Scanner(is);

		try {
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				Entry entry = new Entry(line);
				list.add(entry);
			}
		} finally {
			try {
				scanner.close();
			} catch (Exception e) {
			}
		}

		return list;
	}

	public static List<Entry> find(List<Entry> list, int seqId, int start,
			int span) {
		Entry query = new Entry();
		query.sequenceId = seqId;
		query.alignmentStart = start;
		query.alignmentSpan = span;
		query.containerStartOffset = Long.MAX_VALUE;
		query.sliceOffset = Integer.MAX_VALUE;
		query.sliceSize = Integer.MAX_VALUE;

		int index = Collections.binarySearch(list, query);
		if (index < 0)
			index = -index - 1;
		if (list.get(index).sequenceId != seqId)
			return Collections.EMPTY_LIST;

		query.alignmentStart = start + span;
		int index2 = Collections.binarySearch(list, query);
		if (index2 < 0)
			index2 = -index2 - 1;

		if (index2 <= index)
			index2 = index + 1;

		return list.subList(index, index2);
	}

	public void close() throws IOException {
		os.close();
	}

	public static Entry getLeftmost(List<Entry> list) {
		if (list == null || list.isEmpty())
			return null;
		Entry left = list.get(0);

		for (Entry e : list)
			if (e.alignmentStart < left.alignmentStart)
				left = e;

		return left;

	}

}
