package structure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import net.sf.cram.build.ContainerParser;
import net.sf.cram.build.ContainerFactory;
import net.sf.cram.build.CramIO;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockCompressionMethod;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeaderBLock;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.ContainerHeaderIO;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SliceIO;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;

import org.junit.Test;

public class TestContainer {

	public static byte[] BYTES = new byte[1024 * 1024];

	@Test
	public void testBlock() throws IOException {
		Block b = new Block(BlockCompressionMethod.GZIP, BlockContentType.CORE,
				0, "123457890".getBytes(), null);

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
		s.coreBlock = new Block(BlockCompressionMethod.GZIP,
				BlockContentType.CORE, 0, "core123457890".getBytes(), null);

		s.external = new HashMap<Integer, Block>();
		for (int i = 1; i < 5; i++) {
			Block e1 = new Block(BlockCompressionMethod.GZIP,
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

	private void testALignmentSpan(Container container, CramHeader cramHeader,
			List<CramRecord> records) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		{
			ContainerParser parser = new ContainerParser(cramHeader.samFileHeader);
			for (int i = 0; i < container.slices.length; i++) {
				Slice s = container.slices[i];
				List<CramRecord> list = parser.getRecords(s, container.h);

				CramRecord first = list.get(0);
				CramRecord last = list.get(list.size() - 1);
				assertEquals(s.alignmentStart, first.alignmentStart);
				int end = last.calcualteAlignmentEnd();
				System.out.println(end);
				if (s.alignmentSpan != last.calcualteAlignmentEnd()
						- first.alignmentStart)
					fail(String
							.format("Slice %d alignment span mismatch: %d, %d, %d, %d, %d, %s, %s\n",
									i, s.alignmentStart, s.alignmentSpan,
									first.alignmentStart, last.alignmentStart,
									last.calcualteAlignmentEnd(),
									first.readName, last.readName));

			}
		}

		CramRecord firstRecord1 = records.get(0);
		CramRecord lastRecord1 = records.get(records.size() - 1);
		assertEquals(container.alignmentStart, firstRecord1.alignmentStart);
		if (container.alignmentSpan != lastRecord1.calcualteAlignmentEnd()
				- firstRecord1.alignmentStart)
			fail(String
					.format("Container alignment span mismatch: %d, %d, %d, %d, %d, %s, %s\n",
							container.alignmentStart, container.alignmentSpan,
							firstRecord1.alignmentStart,
							lastRecord1.alignmentStart,
							lastRecord1.calcualteAlignmentEnd(),
							firstRecord1.readName, lastRecord1.readName));
	}

	@Test
	public void test() throws IOException, IllegalArgumentException,
			IllegalAccessException {

		String cramPath = "/data/set1/small.cram";
		InputStream stream = getClass().getResourceAsStream(cramPath);

		if (stream == null)
			fail("CRAM file not found: " + cramPath);

		String refPath = "/data/set1/small.fa";
		InputStream refIS = getClass().getResourceAsStream(cramPath);

		if (refIS == null)
			fail("Ref file not found: " + cramPath);

		Scanner scanner = new Scanner(refIS);
		String refName = scanner.nextLine();
		ByteArrayOutputStream refBAOS = new ByteArrayOutputStream();
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			refBAOS.write(line.getBytes(), 0, line.length());
		}
		byte[] ref = refBAOS.toByteArray();
		refBAOS.close();

		CramHeader cramHeader = CramIO.readCramHeader(stream);
		assertNotNull(cramHeader);
		assertNotNull(cramHeader.samFileHeader);
		assertEquals(2, cramHeader.majorVersion);
		assertEquals(0, cramHeader.minorVersion);

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
		ContainerParser parser = new ContainerParser(cramHeader.samFileHeader);
		parser.getRecords(container, records);

		testALignmentSpan(container, cramHeader, records);

		for (int i = 0; i < records.size(); i++) {
			if (i < 10)
				System.out.println(records.get(i).toString());
		}

		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(
				BYTES);
		CramIO.writeCramHeader(cramHeader, baos);
		byte[] b = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		CramHeader cramHeader2 = CramIO.readCramHeader(bais);
		assertEquals(toString(cramHeader.samFileHeader),
				toString(cramHeader2.samFileHeader));

		ContainerFactory cf = new ContainerFactory(cramHeader.samFileHeader,
				container.slices[0].nofRecords, true);
		Container container2 = cf.buildContainer(records,
				container.h.substitutionMatrix);
		for (int i = 0; i < container.slices.length; i++) {
			container2.slices[i].refMD5 = container.slices[i].refMD5;
		}

		CramIO.writeContainer(container2, baos);

		bais = new ByteArrayInputStream(baos.toByteArray());

		cramHeader2 = CramIO.readCramHeader(bais);
		assertNotNull(cramHeader);
		assertNotNull(cramHeader.samFileHeader);
		assertEquals(2, cramHeader.majorVersion);
		assertEquals(0, cramHeader.minorVersion);

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
		parser.getRecords(container3, records3);
		testALignmentSpan(container3, cramHeader, records3);

		for (int i = 0; i < records3.size(); i++) {
			System.out.println(records3.get(i).toString());
			if (i > 10)
				break;
		}

		assertEquals(container.alignmentStart, container3.alignmentStart);
		assertEquals(container.alignmentSpan, container3.alignmentSpan);
		assertEquals(container.bases, container3.bases);
		assertEquals(container.globalRecordCounter,
				container3.globalRecordCounter);
		assertEquals(container.landmarks.length, container3.landmarks.length);
		assertEquals(container.nofRecords, container3.nofRecords);
		assertEquals(container.sequenceId, container3.sequenceId);

		assertEquals(records.size(), records3.size());
		for (int i = 0; i < records.size(); i++) {
			CramRecord r1 = records.get(i);
			CramRecord r3 = records3.get(i);

			assertTrue(
					"Mismatch at " + i + ":\n" + r1.toString() + "\n"
							+ r3.toString(), compare(r1, r3));
		}

		FileOutputStream fos = new FileOutputStream(new File(
				"./src/test/resources/data/set1/small.cram2"));
		fos.write(baos.getBuffer(), 0, baos.size());
		fos.close();
	}

	private boolean compare(CramRecord r1, CramRecord r2) {
		if (!compare(r1.readName, r2.readName))
			return false;
		if (r1.sequenceId != r2.sequenceId)
			return false;
		if (r1.flags != r2.flags)
			return false;
		if (r1.getMateFlags() != r2.getMateFlags())
			return false;
		if (r1.getCompressionFlags() != r2.getCompressionFlags())
			return false;
		if (r1.alignmentStart != r2.alignmentStart)
			return false;
		if (!compare(r1.readFeatures, r2.readFeatures))
			return false;
		if (!compare(r1.qualityScores, r2.qualityScores))
			return false;
		if (!compare(r1.tags, r2.tags))
			return false;
		if (r1.alignmentDelta != r2.alignmentDelta)
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

			return true;
		}

		if (o1 instanceof List && o2 instanceof List) {
			List l1 = (List) o1;
			List l2 = (List) o2;

			if (!l1.equals(l2))
				return false;
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
