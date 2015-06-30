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
package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import net.sf.cram.build.CramIO;
import net.sf.cram.encoding.reader.BAMRecordView;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.TextCigarCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;

import org.junit.Test;

public class TestBAMRecordView {

	@Test
	public void test() throws IOException {
		byte[] buf = new byte[1024];
		BAMRecordView view = new BAMRecordView(buf);
		view.setRefID(0);
		view.setAlignmentStart(77);
		view.setMappingScore(44);
		view.setIndexBin(99);
		view.setFlags(555);
		view.setMateRefID(0);
		view.setMateAlStart(78);
		view.setInsertSize(133);

		view.setReadName("name1");
		view.setCigar(TextCigarCodec.getSingleton().decode("10M"));
		view.setBases("AAAAAAAAAA".getBytes());
		view.setQualityScores("BBBBBBBBBB".getBytes());

		int id = 'A' << 16 | 'M' << 8 | 'A';
		view.addTag(id, "Q".getBytes(), 0, 1);

		int len = view.finish();

		System.out.println(Arrays.toString(Arrays.copyOf(buf, len)));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		SAMFileHeader header = new SAMFileHeader();
		header.addSequence(new SAMSequenceRecord("14", 14));

		ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
		BAMFileWriter writer = new BAMFileWriter(baos2, null);
		writer.setHeader(header);
		SAMRecord record = new SAMRecord(header);
		record.setReferenceIndex(0);
		record.setAlignmentStart(1);
		record.setCigarString("10M");
		record.setFlags(555);
		record.setMappingQuality(44);
		record.setMateReferenceIndex(0);
		record.setMateAlignmentStart(0);
		record.setInferredInsertSize(133);
		record.setReadName("name1");
		record.setReadBases("AAAAAAAAAA".getBytes());
		record.setBaseQualities("BBBBBBBBBB".getBytes());
		record.setAttribute("AM", 'Q');

		System.out.println("BAMFileWriter.addAlignment():");
		writer.addAlignment(record);
		System.out.println(".");
		writer.close();

		System.out.println("------------------------------------------");
		System.out.println();
		System.out.println(new String(baos2.toByteArray()));
		System.out.println();

		SAMFileReader.setDefaultValidationStringency(ValidationStringency.SILENT);
		SAMFileReader reader2 = new SAMFileReader(new ByteArrayInputStream(baos2.toByteArray()));
		SAMRecordIterator iterator = reader2.iterator();
		while (iterator.hasNext()) {
			record = iterator.next();
			System.out.println(record.getSAMString());
		}
		System.out.println("------------------------------------------");

		BlockCompressedOutputStream bcos = new BlockCompressedOutputStream(baos, null);
		bcos.write("BAM\1".getBytes());
		bcos.write(CramIO.toByteArray(header));
		ByteBufferUtils.writeInt32(header.getSequenceDictionary().size(), bcos);
		for (final SAMSequenceRecord sequenceRecord : header.getSequenceDictionary().getSequences()) {
			byte[] bytes = sequenceRecord.getSequenceName().getBytes();
			ByteBufferUtils.writeInt32(bytes.length + 1, bcos);
			bcos.write(sequenceRecord.getSequenceName().getBytes());
			bcos.write(0);
			ByteBufferUtils.writeInt32(sequenceRecord.getSequenceLength(), bcos);
		}
		bcos.write(buf, 0, len);
		bcos.close();

		System.out.println(new String(baos.toByteArray()));

		SAMFileReader reader = new SAMFileReader(new ByteArrayInputStream(baos.toByteArray()));
		iterator = reader.iterator();
		while (iterator.hasNext()) {
			record = iterator.next();
			System.out.println(record.getSAMString());
		}
		reader.close();

	}

}
