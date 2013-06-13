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
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.index.CramIndex;
import net.sf.cram.index.CramIndex.Entry;

import org.junit.Test;

public class TestIndex {

	@Test
	public void test() throws IOException, CloneNotSupportedException {
		List<CramIndex.Entry> list = new ArrayList<CramIndex.Entry>();

		Entry e = new Entry();
		e.sequenceId = 1;
		e.alignmentStart = 1;
		e.containerStartOffset = 1;
		e.sliceOffset = 1;
		e.sliceSize = 0;
		list.add(e);

		e = e.clone();
		e.alignmentStart = 2;
		e.containerStartOffset = 2;
		e.sliceOffset = 1;
		e.sliceSize = 0;
		list.add(e);

		e = e.clone();
		e.alignmentStart = 3;
		e.containerStartOffset = 3;
		e.sliceOffset = 1;
		e.sliceSize = 0;
		list.add(e);

		System.out.println("Query: 1, 1, 0");
		for (Entry found : CramIndex.find(list, 1, 1, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 1");
		for (Entry found : CramIndex.find(list, 1, 1, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 2");
		for (Entry found : CramIndex.find(list, 1, 1, 2))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 2, 1");
		for (Entry found : CramIndex.find(list, 1, 2, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 0, 2, 1");
		for (Entry found : CramIndex.find(list, 0, 2, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 3, 1");
		for (Entry found : CramIndex.find(list, 1, 4, 1))
			System.out.println(found.toString());
		System.out.println();

		System.out.println("Query: 1, 1, 3");
		for (Entry found : CramIndex.find(list, 1, 1, 3))
			System.out.println(found.toString());
		System.out.println();
	}

}
