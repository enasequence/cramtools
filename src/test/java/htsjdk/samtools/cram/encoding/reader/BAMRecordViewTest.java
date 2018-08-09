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

package htsjdk.samtools.cram.encoding.reader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTagUtil;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class BAMRecordViewTest {

	private int translate(SAMRecord record, BAMRecordView view) {
		view.setReadName(record.getReadName());
		view.setFlags(record.getFlags());
		view.setRefID(record.getReferenceIndex());
		view.setAlignmentStart(record.getAlignmentStart());
		view.setMappingScore(record.getMappingQuality());
		view.setCigar(record.getCigar());
		view.setMateRefID(record.getMateReferenceIndex());
		view.setMateAlStart(record.getMateAlignmentStart());
		view.setInsertSize(record.getInferredInsertSize());
		view.setBases(record.getReadBases());
		view.setQualityScores(record.getBaseQualities());
		view.setTagData(new byte[0], 0, 0);
		return view.finish();
	}

	private void compare(SAMRecord r1, SAMRecord r2) {
		assertThat(r1.getReadName(), equalTo(r2.getReadName()));
		assertThat(r1.getFlags(), equalTo(r2.getFlags()));
		assertThat(r1.getReferenceIndex(), equalTo(r2.getReferenceIndex()));
		assertThat(r1.getAlignmentStart(), equalTo(r2.getAlignmentStart()));
		assertThat(r1.getMappingQuality(), equalTo(r2.getMappingQuality()));
		assertThat(r1.getCigarString(), equalTo(r2.getCigarString()));
		assertThat(r1.getMateReferenceIndex(), equalTo(r2.getMateReferenceIndex()));
		assertThat(r1.getMateAlignmentStart(), equalTo(r2.getMateAlignmentStart()));
		assertThat(r1.getInferredInsertSize(), equalTo(r2.getInferredInsertSize()));
		assertArrayEquals(r1.getReadBases(), r2.getReadBases());
		assertArrayEquals(r1.getBaseQualities(), r2.getBaseQualities());
	}

	private List<SAMRecord> toSAMRecord(BAMRecordView view, SAMFileHeader samHeader) {
		BAMRecordCodec bc = new BAMRecordCodec(samHeader);
		bc.setInputStream(new ByteArrayInputStream(view.buf, 0, view.start));
		List<SAMRecord> records = new ArrayList<SAMRecord>();
		SAMRecord record;
		while ((record = bc.decode()) != null) {
			records.add(record);
		}
		return records;
	}

	@Test
	public void test() {
		SAMFileHeader header = new SAMFileHeader();
		SAMRecord r1 = new SAMRecord(header);
		r1.setReadName("readName");
		r1.setFlags(4);
		r1.setReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
		r1.setAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
		r1.setMappingQuality(SAMRecord.NO_MAPPING_QUALITY);
		r1.setCigar(new Cigar());
		r1.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
		r1.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
		r1.setReadBases("A".getBytes());
		r1.setBaseQualityString("!");

		BAMRecordView view = new BAMRecordView(new byte[1024]);
		translate(r1, view);
		r1.setReadName("2");
		translate(r1, view);

		List<SAMRecord> list = toSAMRecord(view, header);
		assertEquals(2, list.size());

		Iterator<SAMRecord> iterator = list.iterator();
		SAMRecord r2 = iterator.next();
		r1.setReadName("readName");
		compare(r1, r2);

		r1.setReadName("2");
		r2 = iterator.next();
		compare(r1, r2);
	}

	@Test
	public void test1() {
		BAMRecordView view = new BAMRecordView(new byte[1024]);
		view.setReadName("readName");
		view.setFlags(4);
		view.setRefID(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
		view.setAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
		view.setMappingScore(SAMRecord.NO_MAPPING_QUALITY);
		view.setCigar(new Cigar());
		view.setMateRefID(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
		view.setMateAlStart(SAMRecord.NO_ALIGNMENT_START);
		view.setInsertSize(0);
		view.setBases("A".getBytes());
		view.setQualityScores(new byte[] { 0 });
		view.addTag(SAMTagUtil.getSingleton().AM, new byte[] { 'c', 0 }, 0, 1);
		view.finish();

		SAMFileHeader samHeader = new SAMFileHeader();

		BAMRecordCodec bc = new BAMRecordCodec(samHeader);
		bc.setInputStream(new ByteArrayInputStream(view.buf));
		SAMRecord record = bc.decode();
		assertThat(record.getReadName(), is("readName"));
		assertThat(record.getFlags(), is(4));
		assertThat(record.getReferenceIndex(), is(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX));
		assertThat(record.getAlignmentStart(), is(SAMRecord.NO_ALIGNMENT_START));
		assertThat(record.getMappingQuality(), is(SAMRecord.NO_MAPPING_QUALITY));
		assertThat(record.getCigar().getCigarElements().size(), is(0));
		assertThat(record.getMateReferenceIndex(), is(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX));
		assertThat(record.getMateAlignmentStart(), is(SAMRecord.NO_ALIGNMENT_START));
		assertThat(record.getInferredInsertSize(), is(0));
		assertThat(record.getReadString(), is("A"));
		assertThat(record.getBaseQualityString(), is("!"));

		Object amTag = record.getAttribute("AM");
		assertTrue(amTag instanceof Byte);
		Byte amValue = (Byte) amTag;
		assertThat(amValue, equalTo((byte) 0));
	}
}
