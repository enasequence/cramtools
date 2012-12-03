package net.sf.cram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;

public class Index {
	private OutputStream os;

	public Index(OutputStream os) {
		this.os = os;
	}

	public void addContainer(Container c, long offset) throws IOException {
		int i = 0;
		for (Slice s : c.slices) {
			Entry e = new Entry();
			e.sequenceId = c.sequenceId;
			e.alignmentStart = s.alignmentStart;
			e.nofRecords = s.nofRecords;
			e.offset = offset;
			e.slice = i++;

			String string = e.toString();
			os.write(string.getBytes());
			os.write ('\n') ;
		}
	}

	public static class Entry implements Comparable<Entry>, Cloneable {
		public int sequenceId;
		public int alignmentStart;
		public int nofRecords;
		public long offset;
		public long slice;

		public Entry() {
		}

		public Entry(String line) {
			String[] chunks = line.split("\t");
			if (chunks.length != 5)
				throw new RuntimeException("Invalid index format.");

			sequenceId = Integer.valueOf(chunks[0]);
			alignmentStart = Integer.valueOf(chunks[1]);
			nofRecords = Integer.valueOf(chunks[2]);
			offset = Integer.valueOf(chunks[3]);
			slice = Integer.valueOf(chunks[4]);
		}

		@Override
		public String toString() {
			return String.format("%d\t%d\t%d\t%d\t%d", sequenceId,
					alignmentStart, nofRecords, offset, slice);
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
		protected Entry clone() throws CloneNotSupportedException {
			Entry entry = new Entry();
			entry.sequenceId = sequenceId;
			entry.alignmentStart = alignmentStart;
			entry.nofRecords = nofRecords;
			entry.offset = offset;
			entry.slice = slice;
			return entry;
		}
	}

	public static List<Entry> readIndex(InputStream is) {
		List<Entry> list = new LinkedList<Index.Entry>();
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
		query.offset = Long.MAX_VALUE;
		query.slice = Integer.MAX_VALUE;
		int index = Collections.binarySearch(list, query);
		if (index < 0)
			index = -index - 1;
		if (list.get(index).sequenceId != seqId) return Collections.EMPTY_LIST;

		query.alignmentStart = start + span;
		int index2 = Collections.binarySearch(list, query);
		if (index2 < 0)
			index2 = -index2 - 1;

		if (index2 <= index)
			index2 = index+1;

		return list.subList(index, index2);
	}

	public void close() throws IOException {
		os.close();
	}

}
