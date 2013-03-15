package structure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.cram.BLOCK_PROTO;
import net.sf.cram.CramRecord;
import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockCompressionMethod;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeaderBLock;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.ContainerHeaderIO;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SliceIO;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;

import org.junit.Test;

public class TestContainer {

	public static byte[] BYTES = new byte[1024 * 1024];

	@Test
	public void testBlock() throws IOException {
		Block b = new Block(BlockCompressionMethod.GZIP.ordinal(),
				BlockContentType.CORE, 0, "123457890".getBytes(), null);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		b.write(baos);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Block b2 = new Block(bais, true, true);

		assertArrayEquals(b.getRawContent(), b2.getRawContent());
	}

	@Test
	public void testSlices() throws IOException {
		Slice s = new Slice();
		s.alignmentSpan = 13;
		s.alignmentStart = 14;
		s.contentType = BlockContentType.MAPPED_SLICE;
		s.embeddedRefBlockContentID = -1;
		s.globalRecordCounter = 16;
		s.nofRecords = 17;
		s.sequenceId = 18;
		s.refMD5 = "1234567890123456".getBytes();
		s.coreBlock = new Block(BlockCompressionMethod.GZIP.ordinal(),
				BlockContentType.CORE, 0, "core123457890".getBytes(), null);

		s.external = new HashMap<Integer, Block>();
		for (int i = 1; i < 5; i++) {
			Block e1 = new Block(BlockCompressionMethod.GZIP.ordinal(),
					BlockContentType.EXTERNAL, i,
					"external123457890".getBytes(), null);
			s.external.put(i, e1);
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SliceIO sio = new SliceIO();
		sio.write(s, baos);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Slice s2 = new Slice();
		sio.read(s2, bais);

		assertEquals(s.contentIDs.length, s2.external.size());
		assertArrayEquals(s.headerBlock.getRawContent(),
				s2.headerBlock.getRawContent());
		assertArrayEquals(s.coreBlock.getRawContent(),
				s2.coreBlock.getRawContent());

		for (int id : s.contentIDs) {
			Block e1 = s.external.get(id);
			Block e2 = s2.external.get(id);
			assertArrayEquals(e1.getRawContent(), e2.getRawContent());

		}
	}

	@Test
	public void test() throws IOException, IllegalArgumentException,
			IllegalAccessException {

		String cramPath = "/data/set1/small.cram";
		InputStream stream = getClass().getResourceAsStream(cramPath);

		if (stream == null)
			fail("CRAM file not found: " + cramPath);

		CramHeader cramHeader = ReadWrite.readCramHeader(stream);
		assertNotNull(cramHeader);
		assertNotNull(cramHeader.samFileHeader);
		assertEquals(cramHeader.majorVersion, 1);
		assertEquals(cramHeader.minorVersion, 1);

		Container container = new Container();
		ContainerHeaderIO chio = new ContainerHeaderIO();
		chio.readContainerHeader(container, stream);
		assertNotNull(container);
		System.out.println(container);

		CompressionHeaderBLock chb = new CompressionHeaderBLock(stream);
		container.h = chb.getCompressionHeader();

		assertNotNull(container.h);
		System.out.println(container.h);

		SliceIO sio = new SliceIO();
		container.slices = new Slice[container.landmarks.length];
		for (int s = 0; s < container.landmarks.length; s++) {
			Slice slice = new Slice();
			sio.read(slice, stream);
			container.slices[s] = slice;
		}

		System.out.println(container);

		ArrayList<CramRecord> records = new ArrayList<CramRecord>(
				container.nofRecords);
		BLOCK_PROTO.getRecords(container.h, container,
				cramHeader.samFileHeader, records);

		for (int i = 0; i < records.size(); i++) {
			System.out.println(records.get(i).toString());
			if (i > 10)
				break;
		}

		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(
				BYTES);
		ReadWrite.writeCramHeader(cramHeader, baos);
		byte[] b = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		CramHeader cramHeader2 = ReadWrite.readCramHeader(bais);
		assertEquals(toString(cramHeader.samFileHeader),
				toString(cramHeader2.samFileHeader));

		BLOCK_PROTO.recordsPerSlice = container.slices[0].nofRecords;

		Container container2 = BLOCK_PROTO.buildContainer(records,
				cramHeader.samFileHeader, true, 0);
		for (int i = 0; i < container.slices.length; i++)
			container2.slices[i].refMD5 = container.slices[i].refMD5;

		ReadWrite.writeContainer(container2, baos);

		bais = new ByteArrayInputStream(baos.toByteArray());

		cramHeader2 = ReadWrite.readCramHeader(bais);
		assertNotNull(cramHeader);
		assertNotNull(cramHeader.samFileHeader);
		assertEquals(cramHeader.majorVersion, 1);
		assertEquals(cramHeader.minorVersion, 1);

		Container container3 = new Container();
		chio.readContainerHeader(container3, bais);
		assertNotNull(container3);
		System.out.println(container3);

		CompressionHeaderBLock chb3 = new CompressionHeaderBLock(bais);
		container3.h = chb3.getCompressionHeader();

		assertNotNull(container3.h);
		System.out.println(container3.h);

		container3.slices = new Slice[container3.landmarks.length];
		for (int s = 0; s < container3.landmarks.length; s++) {
			Slice slice = new Slice();
			sio.readSliceHeadBlock(slice, bais);
			sio.readSliceBlocks(slice, true, bais);
			container3.slices[s] = slice;
		}

		System.out.println(container3);

		ArrayList<CramRecord> records3 = new ArrayList<CramRecord>(
				container3.nofRecords);
		BLOCK_PROTO.getRecords(container3.h, container3,
				cramHeader.samFileHeader, records3);

		for (int i = 0; i < records3.size(); i++) {
			System.out.println(records3.get(i).toString());
			if (i > 10)
				break;
		}
		
		assertEquals(container.alignmentSpan, container3.alignmentSpan) ;
		assertEquals(container.alignmentStart, container3.alignmentStart) ;
		assertEquals(container.bases, container3.bases) ;
		assertEquals(container.blockCount, container3.blockCount) ;
		assertEquals(container.containerByteSize, container3.containerByteSize) ;
		assertEquals(container.globalRecordCounter, container3.globalRecordCounter) ;
		assertArrayEquals(container.landmarks, container3.landmarks) ;
		assertEquals(container.nofRecords, container3.nofRecords) ;
		assertEquals(container.sequenceId, container3.sequenceId) ;

		assertEquals(records.size(), records3.size());
		for (int i = 0; i < records.size(); i++) {
			CramRecord r1 = records.get(i);
			CramRecord r3 = records3.get(i);

			assertTrue(compare(r1, r3));
		}
	}

	private boolean compare(CramRecord r1, CramRecord r2) {
		if (!compare(r1.getReadName(), r2.getReadName()))
			return false;
		if (r1.sequenceId != r2.sequenceId)
			return false;
		if (r1.getFlags() != r2.getFlags())
			return false;
		if (r1.getMateFlags() != r2.getMateFlags())
			return false;
		if (r1.getCompressionFlags() != r2.getCompressionFlags())
			return false;
		if (r1.getAlignmentStart() != r2.getAlignmentStart())
			return false;
		if (!compare(r1.getReadFeatures(), r2.getReadFeatures()))
			return false;
		if (!compare(r1.getQualityScores(), r2.getQualityScores()))
			return false;
		if (!compare(r1.tags, r2.tags))
			return false;
		if (r1.alignmentStartOffsetFromPreviousRecord != r2.alignmentStartOffsetFromPreviousRecord)
			return false;

		return true;
	}

	private static boolean compare(Object o1, Object o2) {
		if (o1 == null && o2 == null)
			return true;
		if (o1 == null && o2 != null)
			return false;
		if (o1 != null && o2 == null)
			return false;

		if (o1.getClass().isArray()) {
			// Arrays.equals((Object[])o1, (Object[]) o2) ;
			int l1 = Array.getLength(o1);
			int l2 = Array.getLength(o2);
			if (l1 != l2)
				return false;
			for (int i = 0; i < l1; i++)
				if (!compare(Array.get(o1, i), Array.get(o2, i)))
					return false;
			
			return true ;
		}

		return o1.equals(o2);
	}

	private String toString(SAMFileHeader h) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStreamWriter w = new OutputStreamWriter(baos);
		new SAMTextHeaderCodec().encode(w, h);
		try {
			w.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return new String(baos.toByteArray());
	}

}
