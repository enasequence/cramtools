package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.encoding.NullEncoding;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
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
		
		return 4+1+1+20+len ;
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

	private static void writeBlock(Block b, OutputStream os) throws IOException {

		log.debug("WRITING BLOCK: " + b.toString());
		
		b.rawContentSize = b.content.length;
		b.compressedContentSize = b.rawContentSize;
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		switch (b.method) {
		case 0:
			break;
		case 1:
			GZIPOutputStream gos = new GZIPOutputStream(baos);
			gos.write(b.content);
			gos.close();
			b.compressedContentSize = baos.size();
			break;
		default:
			throw new RuntimeException("Unknown compression method: "
					+ b.method);
		}

		ByteBuffer buf = ByteBuffer.allocate(20);
		buf.order(ByteOrder.LITTLE_ENDIAN);

		// method:
		buf.put((byte) b.method);
		// content type id:
		buf.put((byte) b.contentType.ordinal());
		// content id:
		ByteBufferUtils.writeUnsignedITF8(b.contentId, buf);
		// compresses size in bytes:
		ByteBufferUtils.writeUnsignedITF8(b.compressedContentSize, buf);
		// raw size in bytes:
		ByteBufferUtils.writeUnsignedITF8(b.rawContentSize, buf);

		buf.flip();
		byte[] header = new byte[buf.limit()];
		buf.get(header);
		os.write(header);

		if (b.method == 0)
			os.write(b.content);
		else
			os.write(baos.getBuffer(), 0, baos.size());
	}

	private static Block readBlock(InputStream is) throws IOException {
		Block b = new Block();

		int method = is.read();
		b.contentType = BlockContentType.values()[is.read()];
		b.contentId = ByteBufferUtils.readUnsignedITF8(is);
		int compresssedSize = ByteBufferUtils.readUnsignedITF8(is);
		int rawSize = ByteBufferUtils.readUnsignedITF8(is);

		byte[] compressedContent = new byte[compresssedSize];
		DataInputStream dis = new DataInputStream(is);
		dis.readFully(compressedContent);
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedContent);

		switch (method) {
		case 0:
			is = bais;
			break;
		case 1:
			is = new GZIPInputStream(bais);
			break;

		default:
			throw new RuntimeException("Unknown compression method: " + method);
		}

		dis = new DataInputStream(is);
		b.content = new byte[rawSize];
		dis.readFully(b.content);

		log.debug("READ BLOCK: " + b.toString());
		return b;
	}

	private static Block createMappedSliceHeaderBlock(Slice s)
			throws IOException {
		Block b = new Block();
		b.method = 0;
		b.contentType = BlockContentType.MAPPED_SLICE;
		b.contentId = 0;

		// content:
		ByteBuffer content = ByteBuffer.allocate(1024);
		content.order(ByteOrder.LITTLE_ENDIAN);
		ByteBufferUtils.writeUnsignedITF8(s.sequenceId, content);
		ByteBufferUtils.writeUnsignedITF8(s.alignmentStart, content);
		ByteBufferUtils.writeUnsignedITF8(s.alignmentSpan, content);
		ByteBufferUtils.writeUnsignedITF8(s.nofRecords, content);
		// number of block in the slice:
		ByteBufferUtils.writeUnsignedITF8(s.external.size() + 1, content);

		// external ids as array:
		ByteBufferUtils.writeUnsignedITF8(s.external.size(), content);
		for (Integer cid : s.external.keySet())
			ByteBufferUtils.writeUnsignedITF8(cid, content);

		// embeded ref content id
		ByteBufferUtils.writeUnsignedITF8(-1, content);
		content.flip();
		b.content = new byte[content.limit()];
		content.get(b.content);

		return b;
	}

	private static Slice readMappedSlice(LinkedList<Block> blocks)
			throws IOException {
		Block b = blocks.removeFirst();

		if (b.contentType != BlockContentType.MAPPED_SLICE)
			throw new RuntimeException("Unknown slice content type: "
					+ b.contentType.name());

		Slice s = new Slice();
		s.contentType = b.contentType;
		ByteBuffer buf = ByteBuffer.wrap(b.content);
		s.sequenceId = ByteBufferUtils.readUnsignedITF8(buf);
		s.alignmentStart = ByteBufferUtils.readUnsignedITF8(buf);
		s.alignmentSpan = ByteBufferUtils.readUnsignedITF8(buf);
		s.nofRecords = ByteBufferUtils.readUnsignedITF8(buf);
		int blockCount = ByteBufferUtils.readUnsignedITF8(buf);
		int externalCount = ByteBufferUtils.readUnsignedITF8(buf);
		int[] externalIds = new int[externalCount];
		for (int i = 0; i < externalCount; i++)
			externalIds[i] = ByteBufferUtils.readUnsignedITF8(buf);
		int embededRefContentId = ByteBufferUtils.readUnsignedITF8(buf);

		s.coreBlock = blocks.removeFirst();

		s.external = new HashMap<Integer, Block>();
		for (int i = 0; i < externalCount; i++) {
			s.external.put(externalIds[i], blocks.removeFirst());
		}
		return s;
	}

	private static Block createCompressionHeaderBlock(Container c) {
		Block b = new Block();
		b.contentType = BlockContentType.COMPRESSION_HEADER;
		b.contentId = 0;
		b.method = 1;

		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);

		ByteBufferUtils.writeUnsignedITF8(c.sequenceId, buf);
		ByteBufferUtils.writeUnsignedITF8(c.alignmentStart, buf);
		ByteBufferUtils.writeUnsignedITF8(c.alignmentSpan, buf);
		ByteBufferUtils.writeUnsignedITF8(c.nofRecords, buf);
		// landmarks:
		ByteBufferUtils.writeUnsignedITF8(0, buf);

		{ // preservation map:
			ByteBuffer mapBuf = ByteBuffer.allocate(1024 * 100);
			ByteBufferUtils.writeUnsignedITF8(4, mapBuf);
			mapBuf.put("MI".getBytes());
			mapBuf.put((byte) (c.h.mappedQualityScoreIncluded ? 1 : 0));

			mapBuf.put("UI".getBytes());
			mapBuf.put((byte) (c.h.unmappedQualityScoreIncluded ? 1 : 0));

			mapBuf.put("PI".getBytes());
			mapBuf.put((byte) (c.h.unmappedPlacedQualityScoreIncluded ? 1 : 0));

			mapBuf.put("RN".getBytes());
			mapBuf.put((byte) (c.h.readNamesIncluded ? 1 : 0));

			mapBuf.flip();
			byte[] mapBytes = new byte[mapBuf.limit()];
			mapBuf.get(mapBytes);

			ByteBufferUtils.writeUnsignedITF8(mapBytes.length, buf);
			buf.put(mapBytes);
		}

		{ // encoding map:
			ByteBuffer mapBuf = ByteBuffer.allocate(1024 * 100);
			ByteBufferUtils.writeUnsignedITF8(c.h.eMap.size(), mapBuf);
			for (EncodingKey eKey : c.h.eMap.keySet()) {
				mapBuf.put((byte) eKey.name().charAt(0));
				mapBuf.put((byte) eKey.name().charAt(1));

				EncodingParams params = c.h.eMap.get(eKey);
				mapBuf.put((byte) (0xFF & params.id.ordinal()));
				ByteBufferUtils.writeUnsignedITF8(params.params.length, mapBuf);
				mapBuf.put(params.params);
			}
			mapBuf.flip();
			byte[] mapBytes = new byte[mapBuf.limit()];
			mapBuf.get(mapBytes);

			ByteBufferUtils.writeUnsignedITF8(mapBytes.length, buf);
			buf.put(mapBytes);
		}

		{ // tag encoding map:
			ByteBuffer mapBuf = ByteBuffer.allocate(1024 * 100);
			ByteBufferUtils.writeUnsignedITF8(c.h.tMap.size(), mapBuf);
			for (Integer eKey : c.h.tMap.keySet()) {
				ByteBufferUtils.writeUnsignedITF8(eKey, mapBuf);
				
				EncodingParams params = c.h.tMap.get(eKey);
				mapBuf.put((byte) (0xFF & params.id.ordinal()));
				ByteBufferUtils.writeUnsignedITF8(params.params.length, mapBuf);
				mapBuf.put(params.params);
			}
			mapBuf.flip();
			byte[] mapBytes = new byte[mapBuf.limit()];
			mapBuf.get(mapBytes);

			ByteBufferUtils.writeUnsignedITF8(mapBytes.length, buf);
			buf.put(mapBytes);
		}

		buf.flip();
		b.content = new byte[buf.limit()];
		buf.get(b.content);

		return b;
	}

	private static CompressionHeader readCompressionHeader(Block b) {
		CompressionHeader h = new CompressionHeader();

		ByteBuffer buf = ByteBuffer.wrap(b.content);
		// seq id
		ByteBufferUtils.readUnsignedITF8(buf);
		// al start
		ByteBufferUtils.readUnsignedITF8(buf);
		// al span
		ByteBufferUtils.readUnsignedITF8(buf);
		// nof records
		ByteBufferUtils.readUnsignedITF8(buf);

		{ // landmarks:
			int size = ByteBufferUtils.readUnsignedITF8(buf);
			int[] landmarks = new int[size];
			for (int i = 0; i < size; i++) {
				landmarks[i] = ByteBufferUtils.readUnsignedITF8(buf);
			}
		}

		{ // preservation map:
			int byteSize = ByteBufferUtils.readUnsignedITF8(buf);
			int mapSize = ByteBufferUtils.readUnsignedITF8(buf);
			for (int i = 0; i < mapSize; i++) {
				String key = new String(new byte[] { buf.get(), buf.get() });
				if ("MI".equals(key))
					h.mappedQualityScoreIncluded = buf.get() == 1 ? true
							: false;
				else if ("UI".equals(key))
					h.unmappedQualityScoreIncluded = buf.get() == 1 ? true
							: false;
				else if ("PI".equals(key))
					h.unmappedPlacedQualityScoreIncluded = buf.get() == 1 ? true
							: false;
				else if ("RN".equals(key))
					h.readNamesIncluded = buf.get() == 1 ? true : false;
				else
					throw new RuntimeException("Unknown preservation map key: "
							+ key);
			}
		}

		{ // encoding map:
			int byteSize = ByteBufferUtils.readUnsignedITF8(buf);
			int mapSize = ByteBufferUtils.readUnsignedITF8(buf);
			h.eMap = new TreeMap<EncodingKey, EncodingParams>();
			for (EncodingKey key : EncodingKey.values())
				h.eMap.put(key, NullEncoding.toParam());

			for (int i = 0; i < mapSize; i++) {
				String key = new String(new byte[] { buf.get(), buf.get() });
				EncodingKey eKey = EncodingKey.byFirstTwoChars(key);
				if (eKey == null)
					throw new RuntimeException("Unknown encoding key: " + key);

				EncodingID id = EncodingID.values()[buf.get()];
				int paramLen = ByteBufferUtils.readUnsignedITF8(buf);
				byte[] paramBytes = new byte[paramLen];
				buf.get(paramBytes);

				h.eMap.put(eKey, new EncodingParams(id, paramBytes));

				log.debug(String.format("FOUND ENCODING: %s, %s, %s.",
						eKey.name(), id.name(),
						Arrays.toString(Arrays.copyOf(paramBytes, 20))));
			}
		}

		{ // tag encoding map:
			int byteSize = ByteBufferUtils.readUnsignedITF8(buf);
			int mapSize = ByteBufferUtils.readUnsignedITF8(buf);
			h.tMap = new TreeMap<Integer, EncodingParams>();
			for (int i = 0; i < mapSize; i++) {
				int key = ByteBufferUtils.readUnsignedITF8(buf) ;

				EncodingID id = EncodingID.values()[buf.get()];
				int paramLen = ByteBufferUtils.readUnsignedITF8(buf);
				byte[] paramBytes = new byte[paramLen];
				buf.get(paramBytes);

				h.tMap.put(key, new EncodingParams(id, paramBytes));
			}
		}

		return h;
	}

	public static int writeContainer(Container c, OutputStream os)
			throws IOException {

		long time1 = System.nanoTime();
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();

		Block block = createCompressionHeaderBlock(c);
		block.method = 1;
		writeBlock(block, baos);
		c.blockCount = 1;

		List<Integer> landmarks = new ArrayList<Integer>();
		for (int i = 0; i < c.slices.length; i++) {
			Slice s = c.slices[i];
			landmarks.add(baos.size());

			Block sliceBlock = createMappedSliceHeaderBlock(s);
			sliceBlock.method = 0;
			writeBlock(sliceBlock, baos);
			s.coreBlock.method = 1;
			writeBlock(s.coreBlock, baos);
			for (Integer contentId : s.external.keySet()) {
				Block b = s.external.get(contentId);
				b.method = 1;
				writeBlock(b, baos);
			}
			c.blockCount += 2 + s.external.size();
		}
		c.landmarks = new int[landmarks.size()];
		for (int i = 0; i < c.landmarks.length; i++)
			c.landmarks[i] = landmarks.get(i);

		ByteBuffer buf = ByteBuffer.allocate(1024);
		ByteBufferUtils.writeUnsignedITF8(baos.size(), buf);
		ByteBufferUtils.writeUnsignedITF8(c.sequenceId, buf);
		ByteBufferUtils.writeUnsignedITF8(c.alignmentStart, buf);
		ByteBufferUtils.writeUnsignedITF8(c.alignmentSpan, buf);
		ByteBufferUtils.writeUnsignedITF8(c.nofRecords, buf);
		ByteBufferUtils.writeUnsignedITF8(c.blockCount, buf);
		ByteBufferUtils.writeUnsignedITF8(c.landmarks.length, buf);
		for (int i = 0; i < c.landmarks.length; i++)
			ByteBufferUtils.writeUnsignedITF8(c.landmarks[i], buf);

		buf.flip();
		byte[] header = new byte[buf.limit()];
		buf.get(header);

		os.write(header);
		os.write(baos.getBuffer(), 0, baos.size());

		long time2 = System.nanoTime();

		log.debug("CONTAINER WRITTEN: " + c.toString());
		c.writeTime = time2 - time1;
		
		return header.length + baos.size() ;
	}

	public static Container readContainer(SAMFileHeader samFileHeader,
			InputStream is) throws IOException {
		return readContainer(samFileHeader, is, 0, Integer.MAX_VALUE);
	}

	public static Container readContainer(SAMFileHeader samFileHeader,
			InputStream is, int fromBlock, int howManySlices)
			throws IOException {

		long time1 = System.nanoTime();
		Container c = new Container();
		int containerByteSize = ByteBufferUtils.readUnsignedITF8(is);
		c.sequenceId = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentStart = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentSpan = ByteBufferUtils.readUnsignedITF8(is);
		c.nofRecords = ByteBufferUtils.readUnsignedITF8(is);
		c.blockCount = ByteBufferUtils.readUnsignedITF8(is);
		c.landmarks = new int[ByteBufferUtils.readUnsignedITF8(is)];
		for (int i = 0; i < c.landmarks.length; i++)
			c.landmarks[i] = ByteBufferUtils.readUnsignedITF8(is);

		if (fromBlock > 0)
			is.skip(c.landmarks[fromBlock]);

		LinkedList<Block> blocks = new LinkedList<Block>();
		for (int i = 0; i < c.blockCount; i++) {
			blocks.add(readBlock(is));
		}

		c.h = readCompressionHeader(blocks.removeFirst());

		List<Slice> slices = new ArrayList<Slice>();
		while (!blocks.isEmpty()) {
			slices.add(readMappedSlice(blocks));
		}

		c.slices = (Slice[]) slices.toArray(new Slice[slices.size()]);

		long time2 = System.nanoTime();

		log.debug("READ CONTAINER: " + c.toString());
		c.readTime = time2 - time1;
		
		return c;
	}

	private static long writeContainer(SAMFileHeader samFileHeader,
			OutputStream os) throws IOException {
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		OutputStreamWriter w = new OutputStreamWriter(baos);
		new SAMTextHeaderCodec().encode(w, samFileHeader);
		w.close();

		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(baos.size());
		buf.flip();
		byte[] bytes = new byte[buf.limit()];
		buf.get(bytes);

		os.write(bytes);
		os.write(baos.getBuffer(), 0, baos.size());
		
		return bytes.length + baos.size() ;
	}

	private static SAMFileHeader readSAMFileHeader(String id, InputStream is)
			throws IOException {
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
