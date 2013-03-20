package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockCompressionMethod;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeaderBLock;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.ContainerHeaderIO;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SliceIO;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.util.BufferedLineReader;

public class ReadWrite {
	private static final byte[] CHECK = "".getBytes();
	private static Log log = Log.getInstance(ReadWrite.class);

	private static final boolean check(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		byte[] bytes = new byte[CHECK.length];
		dis.readFully(bytes);

		boolean result = Arrays.equals(CHECK, bytes);

		if (!result)
			log.error("Expected %s but got %s.\n", new String(CHECK),
					new String(bytes));

		return result;
	}

	private static final void check(OutputStream os) throws IOException {
		os.write(CHECK);
	}

	public static final class CramHeader {

		public static final byte[] magick = "CRAM".getBytes();
		public byte majorVersion;
		public byte minorVersion;
		public final byte[] id = new byte[20];

		public SAMFileHeader samFileHeader;

		private CramHeader() {
		}

		public CramHeader(int majorVersion, int minorVersion, String id,
				SAMFileHeader samFileHeader) {
			this.majorVersion = (byte) majorVersion;
			this.minorVersion = (byte) minorVersion;
			System.arraycopy(id.getBytes(), 0, this.id, 0,
					Math.min(id.length(), this.id.length));
			this.samFileHeader = samFileHeader;
		}

	}

	public static long writeCramHeader(CramHeader h, OutputStream os)
			throws IOException {
		os.write("CRAM".getBytes("US-ASCII"));
		os.write(h.majorVersion);
		os.write(h.minorVersion);
		os.write(h.id);

		long len = writeContainer(h.samFileHeader, os);

		return 4 + 1 + 1 + 20 + len;
	}

	public static CramHeader readCramHeader(InputStream is) throws IOException {
		CramHeader h = new CramHeader();
		for (byte b : CramHeader.magick) {
			if (b != is.read())
				throw new RuntimeException("Unknown file format.");
		}

		h.majorVersion = (byte) is.read();
		h.minorVersion = (byte) is.read();

		DataInputStream dis = new DataInputStream(is);
		dis.readFully(h.id);

		h.samFileHeader = readSAMFileHeader(new String(h.id), is);
		return h;
	}

	public static int writeContainer(Container c, OutputStream os)
			throws IOException {

		long time1 = System.nanoTime();
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();

		Block block = new CompressionHeaderBLock(c.h);
		block.write(baos);
		c.blockCount = 1;

		List<Integer> landmarks = new ArrayList<Integer>();
		SliceIO sio = new SliceIO();
		for (int i = 0; i < c.slices.length; i++) {
			Slice s = c.slices[i];
			landmarks.add(baos.size());
			sio.write(s, baos);
			c.blockCount++ ;
			c.blockCount++;
			if (s.embeddedRefBlock != null)
				c.blockCount++;
			c.blockCount += s.external.size();
		}
		c.landmarks = new int[landmarks.size()];
		for (int i = 0; i < c.landmarks.length; i++)
			c.landmarks[i] = landmarks.get(i);

		c.containerByteSize = baos.size();
		calculateSliceOffsetsAndSizes(c);

		ContainerHeaderIO chio = new ContainerHeaderIO();
		int len = chio.writeContainerHeader(c, os);
		os.write(baos.getBuffer(), 0, baos.size());
		len += baos.size();

		long time2 = System.nanoTime();

		log.debug("CONTAINER WRITTEN: " + c.toString());
		c.writeTime = time2 - time1;

		return len;
	}

	public static Container readContainer(SAMFileHeader samFileHeader,
			InputStream is) throws IOException {
		return readContainer(samFileHeader, is, 0, Integer.MAX_VALUE);
	}

	public static Container readContainerHeader(InputStream is)
			throws IOException {
		Container c = new Container();
		ContainerHeaderIO chio = new ContainerHeaderIO();
		if (!chio.readContainerHeader(c, is)) return null ;
		return c;
	}

	public static Container readContainer(SAMFileHeader samFileHeader,
			InputStream is, int fromSlice, int howManySlices)
			throws IOException {

		long time1 = System.nanoTime();
		Container c = readContainerHeader(is);
		if (c == null) return null ;

		CompressionHeaderBLock chb = new CompressionHeaderBLock(is);
		c.h = chb.getCompressionHeader();
		howManySlices = Math.min(c.landmarks.length, howManySlices);

		if (fromSlice > 0)
			is.skip(c.landmarks[fromSlice]);

		SliceIO sio = new SliceIO();
		List<Slice> slices = new ArrayList<Slice>();
		for (int s = fromSlice; s < howManySlices - fromSlice; s++) {
			Slice slice = new Slice();
			sio.readSliceHeadBlock(slice, is);
			sio.readSliceBlocks(slice, true, is);
			slices.add(slice) ;
		}

		c.slices = (Slice[]) slices.toArray(new Slice[slices.size()]);

		calculateSliceOffsetsAndSizes(c);

		long time2 = System.nanoTime();

		log.debug("READ CONTAINER: " + c.toString());
		c.readTime = time2 - time1;

		return c;
	}

	private static void calculateSliceOffsetsAndSizes(Container c) {
		for (int i = 0; i < c.slices.length - 1; i++) {
			Slice s = c.slices[i];
			s.offset = c.landmarks[i];
			s.size = c.landmarks[i + 1] - s.offset;
		}
		Slice lastSlice = c.slices[c.slices.length - 1];
		lastSlice.offset = c.landmarks[c.landmarks.length - 1];
		lastSlice.size = c.containerByteSize - lastSlice.offset;
	}

	private static byte[] toByteArray(SAMFileHeader samFileHeader) {
		ExposedByteArrayOutputStream headerBodyOS = new ExposedByteArrayOutputStream();
		OutputStreamWriter w = new OutputStreamWriter(headerBodyOS);
		new SAMTextHeaderCodec().encode(w, samFileHeader);
		try {
			w.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(headerBodyOS.size());
		buf.flip();
		byte[] bytes = new byte[buf.limit()];
		buf.get(bytes);

		ByteArrayOutputStream headerOS = new ExposedByteArrayOutputStream();
		try {
			headerOS.write(bytes);
			headerOS.write(headerBodyOS.getBuffer(), 0, headerBodyOS.size());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return headerOS.toByteArray();
	}

	private static long writeContainer(SAMFileHeader samFileHeader,
			OutputStream os) throws IOException {
		Block block = new Block();
		block.setRawContent(toByteArray(samFileHeader));
		block.method = BlockCompressionMethod.RAW.ordinal();
		block.contentId = 0;
		block.contentType = BlockContentType.FILE_HEADER;
		block.compress();

		Container c = new Container();
		c.blockCount = 1;
		c.blocks = new Block[] { block };
		c.landmarks = new int[0];
		c.slices = new Slice[0];
		c.alignmentSpan = 0;
		c.alignmentStart = 0;
		c.bases = 0;
		c.globalRecordCounter = 0;
		c.nofRecords = 0;
		c.sequenceId = 0;

		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		block.write(baos);
		c.containerByteSize = baos.size();

		ContainerHeaderIO chio = new ContainerHeaderIO();
		int len = chio.writeContainerHeader(c, os);
		os.write(baos.getBuffer(), 0, baos.size());

		return len + baos.size();
	}

	private static SAMFileHeader readSAMFileHeader(String id, InputStream is)
			throws IOException {
		Container readContainerHeader = readContainerHeader(is);
		Block b = new Block(is, true, true);

		is = new ByteArrayInputStream(b.getRawContent());

		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		for (int i = 0; i < 4; i++)
			buf.put((byte) is.read());
		buf.flip();
		int size = buf.asIntBuffer().get();

		DataInputStream dis = new DataInputStream(is);
		byte[] bytes = new byte[size];
		dis.readFully(bytes);

		BufferedLineReader r = new BufferedLineReader(new ByteArrayInputStream(
				bytes));
		return new SAMTextHeaderCodec().decode(r, id);
	}
}
