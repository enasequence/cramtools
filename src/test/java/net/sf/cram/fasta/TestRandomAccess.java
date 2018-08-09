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

import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;

public class TestRandomAccess {
	private ReferenceSequenceFile f1;
	private ReferenceSequenceFile f2;
	private String seq;

	public TestRandomAccess(File file1, File file2, String seq) throws FileNotFoundException {
		this.seq = seq;
		f1 = new BGZF_ReferenceSequenceFile(file1);
		f2 = new IndexedFastaSequenceFile(file2);
	}

	public void test() throws FileNotFoundException {

		// ReferenceSequence s1 = f1.getSequence(seq);
		// assertNotNull(s1);
		// ReferenceSequence s2 = f2.getSequence(seq);
		// assertNotNull(s2);
		//
		// assertArrayEquals(s1.getBases(), s2.getBases());

		int end = 249249621;
		for (int start = Math.max(1, end - 200); start < end; start++) {
			System.out.printf("%s:%d\t", seq, start);
			for (int stop = start; stop < start + 200 && stop <= end; stop++) {
				ReferenceSequence t1 = f1.getSubsequenceAt(seq, start, stop);
				assertNotNull(t1);
				ReferenceSequence t2 = f2.getSubsequenceAt(seq, start, stop);
				assertNotNull(t2);

				assertArrayEquals(t1.getBases(), t2.getBases());

				// byte[] ss1 = Arrays.copyOfRange(s1.getBases(), start - 1,
				// stop);
				// assertArrayEquals(ss1, t1.getBases());
				//
				// byte[] ss2 = Arrays.copyOfRange(s2.getBases(), start - 1,
				// stop);
				// assertArrayEquals(ss2, t2.getBases());

			}
			System.out.println("OK");
		}
		System.out.println("All OK.");
	}

	public static void main(String[] args) throws FileNotFoundException {
		File file1 = new File(args[0]);
		File file2 = new File(args[1]);
		String seq = args[2];

		new TestRandomAccess(file1, file2, seq).test();
	}

}
