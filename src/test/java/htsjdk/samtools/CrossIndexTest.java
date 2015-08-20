package htsjdk.samtools;

import static org.junit.Assert.assertTrue;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CrossIndexTest {
	static Log log = Log.getInstance(CrossIndexTest.class);

	public static void main(String[] args) throws IOException {
		Log.setGlobalLogLevel(LogLevel.INFO);
		File cramFile = new File(args[0]);
		File refFile = new File(args[1]);
		File craiFile = new File(cramFile.getAbsolutePath() + ".crai");
		File baiFile = new File(cramFile.getAbsolutePath() + ".bai");

		assertTrue(cramFile.isFile());
		assertTrue(craiFile.isFile());
		assertTrue(baiFile.isFile());
		assertTrue(refFile.isFile());

		SeekableStream cramStream = new SeekableFileStream(cramFile);
		CRAMFileReader reader = new CRAMFileReader(cramFile, new ReferenceSource(refFile));
		IndexAggregate indexAgg = IndexAggregate
				.forDataFile(cramStream, reader.getFileHeader().getSequenceDictionary());

		IndexAggregate craiAgg = IndexAggregate.fromCraiFile(new FileInputStream(craiFile), reader.getFileHeader()
				.getSequenceDictionary());

		IndexAggregate baiAgg = IndexAggregate.fromBaiFile(new SeekableFileStream(baiFile), reader.getFileHeader()
				.getSequenceDictionary());

		SAMSequenceRecord seq = reader.getFileHeader().getSequence(19);
		// {
		// // 2669001-2669057
		// long craiOffset = craiAgg.seek(19, 2669001, 2669057, cramStream);
		// long baiOffset = baiAgg.seek(19, 2669001, 2669057, cramStream);
		// System.out.println(craiOffset);
		// System.out.println(baiOffset);
		// }
		// {
		// // 19:4163001-4163002
		// long craiOffset = craiAgg.seek(19, 4163001, 4163002, cramStream);
		// long baiOffset = baiAgg.seek(19, 4163001, 4163002, cramStream);
		// System.out.println(craiOffset);
		// System.out.println(baiOffset);
		// }
		// System.exit(1);

		Set<Long> offsets = new HashSet<Long>();
		long time = System.currentTimeMillis();

		int seqId = 19;
		for (int span = 1; span < 100000; span++) {
			for (int start = 1; start < seq.getSequenceLength() - span; start += 1000) {
				long craiOffset = craiAgg.seek(seqId, start, start + span, cramStream);
				long baiOffset = baiAgg.seek(seqId, start, start + span, cramStream);
				long offset = indexAgg.seek(seqId, start, start + span, cramStream);

				if (craiOffset != baiOffset || craiOffset != offset)
					throw new RuntimeException(String.format("Offsets mismatch for query: %d:%d-%d", seqId, start,
							start + span));

				offsets.add(craiOffset);

				if (System.currentTimeMillis() - time > 10000) {
					log.info(String.format("Tested: start=%d, span=%d, unique offsets=%d", start, span, offsets.size()));
					time = System.currentTimeMillis();
				}
			}
		}

		for (long offset : offsets) {
			System.out.println(offset);
		}
	}
}
