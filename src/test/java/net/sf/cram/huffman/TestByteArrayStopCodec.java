package net.sf.cram.huffman;

import java.awt.List;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.encoding.ByteArrayStopEncoding;
import net.sf.cram.encoding.ExternalByteArrayCodec;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;

import org.junit.Test;

public class TestByteArrayStopCodec {

	@Test
	public void test1() throws IOException {
		SAMFileReader reader = new SAMFileReader(
				new File(
						"c:/temp/HG00096.mapped.illumina.mosaik.GBR.exome.20110411.chr20.bam"));
		SAMRecordIterator iterator = reader.iterator();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(baos);
		BufferedOutputStream bos = new BufferedOutputStream(gos) ;
		ByteArrayStopEncoding.ByteArrayStopCodec c = new ByteArrayStopEncoding.ByteArrayStopCodec(
				(byte) 0, null, bos);
		int maxRecords = 100000;
		int counter = 0;

		long writeNanos = 0;
		while (iterator.hasNext() && counter++ < maxRecords) {
			SAMRecord r = iterator.next();
			byte[] scores = r.getBaseQualities();
			long time1 = System.nanoTime();
			c.write(null, scores);
			writeNanos += System.nanoTime() - time1;
		}
		bos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		GZIPInputStream gis = new GZIPInputStream(bais);
		c = new ByteArrayStopEncoding.ByteArrayStopCodec((byte) 0,
				new BufferedInputStream(gis), null);

		long bases = 0;
		long time3 = System.nanoTime();
		for (counter = 0; counter < maxRecords; counter++)
			bases += c.read(null).length;
		long time4 = System.nanoTime();

		System.out.printf("ByteArrayStopCodec: bases %d, %d ms, %d ms.\n", bases,
				(writeNanos) / 1000000, (time4 - time3) / 1000000);
		reader.close();
	}

	@Test
	public void test2() throws IOException {
		SAMFileReader reader = new SAMFileReader(
				new File(
						"c:/temp/HG00096.mapped.illumina.mosaik.GBR.exome.20110411.chr20.bam"));
		SAMRecordIterator iterator = reader.iterator();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(baos);
		BufferedOutputStream bos = new BufferedOutputStream(gos) ;

		ExternalByteArrayCodec c = new ExternalByteArrayCodec(
				bos, null);
		int maxRecords = 100000;
		int counter = 0;

		long writeNanos = 0;
		java.util.List<SAMRecord> records = new ArrayList<SAMRecord>(2*maxRecords) ;
		while (iterator.hasNext() && counter++ <= maxRecords) {
			SAMRecord r = iterator.next();
			records.add(r) ;
			byte[] scores = r.getBaseQualities();
			long time1 = System.nanoTime();
			c.write(null, scores);
			writeNanos += System.nanoTime() - time1;
		}
		bos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		GZIPInputStream gis = new GZIPInputStream(bais);
		c = new ExternalByteArrayCodec(null, new BufferedInputStream(gis));

		long bases = 0;
		long readNanos = 0 ;
		for (counter = 0; counter < records.size(); counter++) {
			SAMRecord record = records.get(counter) ;
				
			long time4 = System.nanoTime();
			bases += c.read(null, record.getReadLength()).length;
			readNanos += System.nanoTime() - time4 ;
		}

		System.out.printf("ExternalByteArrayCodec: bases %d, %d ms, %d ms.\n", bases,
				(writeNanos) / 1000000, (readNanos) / 1000000);
		reader.close();
	}

}
