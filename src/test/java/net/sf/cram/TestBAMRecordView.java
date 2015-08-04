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
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;


import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.encoding.reader.BAMRecordView;
import htsjdk.samtools.cram.io.CramInt;
import htsjdk.samtools.cram.io.ExposedByteArrayOutputStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;
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
		view.setCigar(TextCigarCodec.decode("10M"));
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
		SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(header, true, baos2);
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
		bcos.write(toByteArray(header));
		CramInt.writeInt32(header.getSequenceDictionary().size(), bcos);
		for (final SAMSequenceRecord sequenceRecord : header.getSequenceDictionary().getSequences()) {
			byte[] bytes = sequenceRecord.getSequenceName().getBytes();
			CramInt.writeInt32(bytes.length + 1, bcos);
			bcos.write(sequenceRecord.getSequenceName().getBytes());
			bcos.write(0);
			CramInt.writeInt32(sequenceRecord.getSequenceLength(), bcos);
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

	private static byte[] toByteArray(SAMFileHeader samFileHeader) {
		ExposedByteArrayOutputStream headerBodyOS = new ExposedByteArrayOutputStream();
		OutputStreamWriter outStreamWriter = new OutputStreamWriter(headerBodyOS);
		(new SAMTextHeaderCodec()).encode(outStreamWriter, samFileHeader);

		try {
			outStreamWriter.close();
		} catch (IOException var8) {
			throw new RuntimeException(var8);
		}

		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(headerBodyOS.size());
		buf.flip();
		byte[] bytes = new byte[buf.limit()];
		buf.get(bytes);
		ByteArrayOutputStream headerOS = new ByteArrayOutputStream();

		try {
			headerOS.write(bytes);
			headerOS.write(headerBodyOS.getBuffer(), 0, headerBodyOS.size());
		} catch (IOException var7) {
			throw new RuntimeException(var7);
		}

		return headerOS.toByteArray();
	}

}
