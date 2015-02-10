package net.sf.cram.encoding.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import net.sf.cram.Bam2Cram;
import net.sf.cram.build.CramIO;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.Slice;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;

import org.junit.Test;

public class TestReaderToFasta {

	@Test
	public void test() throws IOException, IllegalArgumentException,
			IllegalAccessException, NoSuchAlgorithmException {
		String pathToRefFile = "src/test/resources/data/set1/small.fa";
		String pathToBamFile = "src/test/resources/data/set1/small.bam";

		SAMFileReader samFileReader = new SAMFileReader(new File(pathToBamFile));
		Map<String, byte[]> readMap = new HashMap<String, byte[]>();
		for (SAMRecord record : samFileReader) {
			readMap.put(
					record.getReadName()
							+ (record.getReadPairedFlag() ? (record
									.getFirstOfPairFlag() ? "/1" : "/2") : ""),
					record.getReadBases());
		}
		samFileReader.close();

		ReferenceSource referenceSource = new ReferenceSource(new File(
				pathToRefFile));
		File cramFile = File.createTempFile("bam", ".cram");

		Bam2Cram.main(String.format("-l error -I %s -R %s -O %s",
				pathToBamFile, pathToRefFile, cramFile.getAbsolutePath())
				.split(" "));

		InputStream is = new FileInputStream(cramFile);
		ReaderToFasta reader = new ReaderToFasta();
		reader.reverseNegativeReads = false;
		reader.appendSegmentIndexToReadNames = true ;

		CramHeader cramHeader = CramIO.readCramHeader(is);
		Container container = null;
		byte[] ref = null;

		while ((container = CramIO.readContainer(is)) != null) {
			DataReaderFactory f = new DataReaderFactory();

			for (Slice s : container.slices) {
				if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					SAMSequenceRecord sequence = cramHeader.samFileHeader
							.getSequence(s.sequenceId);
					ref = referenceSource.getReferenceBases(sequence, true);
				}
				Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (Integer exId : s.external.keySet()) {
					inputMap.put(exId,
							new ByteArrayInputStream(s.external.get(exId)
									.getRawContent()));
				}

				reader.referenceSequence = ref;
				reader.prevAlStart = s.alignmentStart;
				reader.substitutionMatrix = container.h.substitutionMatrix;
				reader.recordCounter = 0;
				reader.appendSegmentIndexToReadNames = true;
				f.buildReader(reader, new DefaultBitInputStream(
						new ByteArrayInputStream(s.coreBlock.getRawContent())),
						inputMap, container.h, s.sequenceId);

				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
				}

				for (ByteBuffer buf : reader.bufs) {
					buf.flip();
					byte[] bytes = new byte[buf.limit()];
					buf.get(bytes);

					Scanner scanner = new Scanner(new ByteArrayInputStream(
							bytes));
					while (scanner.hasNextLine()) {
						String name = scanner.nextLine().substring(1);
						String baseString = scanner.nextLine();

						assertTrue(name, readMap.containsKey(name));
						byte[] bases = readMap.remove(name);
						assertEquals(name, new String(bases), baseString);
					}
					scanner.close();

					buf.clear();
				}
			}
		}

	}

}
