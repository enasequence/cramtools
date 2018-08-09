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

package net.sf.cram;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.util.SequenceUtil;

import java.io.IOException;
import java.util.Arrays;

import net.sf.cram.ref.ReferenceRegion;
import net.sf.cram.ref.ReferenceSource;

import org.junit.Assert;
import org.junit.Test;

public class AlignmentsTagsTest {

	@Test
	public void test() throws IOException, CloneNotSupportedException {
		byte[] ref = "ACGT".getBytes();
		doTest(ref, 1, ref);

		doTest("ACGT".getBytes(), 1, "CCGT".getBytes());
		doTest("ACGT".getBytes(), 2, "CGT".getBytes());
		doTest("ACGT".getBytes(), 2, "AGT".getBytes());
		doTest("ACGT".getBytes(), 2, "AGC".getBytes());
	}

	private void doTest(byte[] ref, int alignmentStart, byte[] readBases) throws IOException,
			CloneNotSupportedException {
		SAMSequenceRecord sequenceRecord = new SAMSequenceRecord("1", ref.length);
		SAMSequenceDictionary sequenceDictionary = new SAMSequenceDictionary();
		sequenceDictionary.addSequence(sequenceRecord);

		SAMFileHeader header = new SAMFileHeader();
		header.setSequenceDictionary(sequenceDictionary);
		SAMRecord samRecord = new SAMRecord(header);
		samRecord.setReadUnmappedFlag(false);
		samRecord.setAlignmentStart(alignmentStart);
		samRecord.setReferenceIndex(0);
		samRecord.setReadBases(readBases);
		samRecord.setCigarString(samRecord.getReadLength() + "M");

		ReferenceSource referenceSource = new ReferenceSource() {
			@Override
			public synchronized ReferenceRegion getRegion(SAMSequenceRecord record, int start_1based,
					int endInclusive_1based) throws IOException {
				int zbInclusiveStart = start_1based - 1;
				int zbExlcusiveEnd = endInclusive_1based;
				return new ReferenceRegion(Arrays.copyOfRange(ref, zbInclusiveStart, zbExlcusiveEnd),
						sequenceRecord.getSequenceIndex(), sequenceRecord.getSequenceName(), start_1based);
			}
		};

		AlignmentsTags.calculateMdAndNmTags(samRecord, referenceSource, sequenceDictionary, true, true);

		SAMRecord checkRecord = (SAMRecord) samRecord.clone();
		SequenceUtil.calculateMdAndNmTags(checkRecord, ref, true, true);
		// System.out.printf("TEST: ref %s, start %d, read bases %s\n", new
		// String(ref), alignmentStart, new String(
		// readBases));
		// System.out
		// .println(referenceSource.getRegion(sequenceRecord, alignmentStart,
		// alignmentStart + readBases.length));
		// System.out.printf("NM:  %s x %s\n", samRecord.getAttribute("NM"),
		// checkRecord.getAttribute("NM"));
		// System.out.printf("MD: %s x %s\n", samRecord.getAttribute("MD"),
		// checkRecord.getAttribute("MD"));

		Assert.assertEquals(checkRecord.getAttribute("NM"), samRecord.getAttribute("NM"));
		Assert.assertEquals(checkRecord.getAttribute("MD"), samRecord.getAttribute("MD"));
	}
}
