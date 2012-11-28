package net.sf.cram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.Index.Entry;

import org.junit.Test;

public class TestIndex {

	@Test
	public void test() throws IOException, CloneNotSupportedException {
		List<Index.Entry> list = new ArrayList<Index.Entry>();

		Entry e = new Entry();
		e.sequenceId = 1;
		e.alignmentStart = 1;
		e.nofRecords = 1;
		e.offset = 1;
		e.slice = 0;
		list.add(e);

		e = e.clone();
		e.alignmentStart = 2;
		e.offset = 2;
		list.add(e);

		e = e.clone();
		e.alignmentStart = 3;
		e.offset = 3;
		list.add(e);

		System.out.println("Query: 1, 1, 0");
		for (Entry found : Index.find(list, 1, 1, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 1");
		for (Entry found : Index.find(list, 1, 1, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 2");
		for (Entry found : Index.find(list, 1, 1, 2))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 2, 1");
		for (Entry found : Index.find(list, 1, 2, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 0, 2, 1");
		for (Entry found : Index.find(list, 0, 2, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 3, 1");
		for (Entry found : Index.find(list, 1, 4, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 3");
		for (Entry found : Index.find(list, 1, 1, 3))
			System.out.println(found.toString());
		System.out.println();
	}

}
