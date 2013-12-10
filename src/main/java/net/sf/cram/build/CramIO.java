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
package net.sf.cram.build;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.CountingInputStream;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockCompressionMethod;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeaderBLock;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.ContainerHeaderIO;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SliceIO;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.util.BufferedLineReader;
import net.sf.samtools.util.SeekableStream;

public class CramIO {
	public static int DEFINITION_LENGTH = 4 + 1 + 1 + 20;
	private static final byte[] CHECK = "".getBytes();
	private static Log log = Log.getInstance(CramIO.class);
	public static byte[] ZERO_B_EOF_MARKER = ByteBufferUtils
			.bytesFromHex("0b 00 00 00 ff ff ff ff ff e0 45 4f 46 00 00 00 00 01 00 00 01 00 06 06 01 00 01 00 01 00");

	private static final boolean check(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		byte[] bytes = new byte[CHECK.length];
		dis.readFully(bytes);

		boolean result = Arrays.equals(CHECK, bytes);

		if (!result)
			log.error("Expected %s but got %s.\n", new String(CHECK), new String(bytes));

		return result;
	}

	private static final void check(OutputStream os) throws IOException {
		os.write(CHECK);
	}

	public static long issueZeroB_EOF_marker(OutputStream os) throws IOException {
		os.write(ZERO_B_EOF_MARKER);
		return ZERO_B_EOF_MARKER.length;
	}

	public static boolean hasZeroB_EOF_marker(SeekableStream s) throws IOException {
		byte[] tail = new byte[ZERO_B_EOF_MARKER.length];

		s.seek(s.length() - ZERO_B_EOF_MARKER.length);
		ByteBufferUtils.readFully(tail, s);

		return Arrays.equals(tail, ZERO_B_EOF_MARKER);
	}

	public static boolean hasZeroB_EOF_marker(File file) throws IOException {
		byte[] tail = new byte[ZERO_B_EOF_MARKER.length];

		RandomAccessFile raf = new RandomAccessFile(file, "r");
		try {
			raf.seek(file.length() - ZERO_B_EOF_MARKER.length);
			raf.readFully(tail);
		} catch (IOException e) {
			throw e;
		} finally {
			raf.close();
		}

		return Arrays.equals(tail, ZERO_B_EOF_MARKER);
	}

	public static long writeCramHeader(CramHeader h, OutputStream os) throws IOException {
		os.write("CRAM".getBytes("US-ASCII"));
		os.write(h.majorVersion);
		os.write(h.minorVersion);
		os.write(h.id);
		for (int i = h.id.length; i < 20; i++)
			os.write(0);

		long len = writeContainerForSamFileHeader(h.samFileHeader, os);

		return DEFINITION_LENGTH + len;
	}

	private static void readFormatDefinition(InputStream is, CramHeader header) throws IOException {
		for (byte b : CramHeader.magick) {
			if (b != is.read())
				throw new RuntimeException("Unknown file format.");
		}

		header.majorVersion = (byte) is.read();
		header.minorVersion = (byte) is.read();

		DataInputStream dis = new DataInputStream(is);
		dis.readFully(header.id);
	}

	public static CramHeader readCramHeader(InputStream is) throws IOException {
		CramHeader header = new CramHeader();

		readFormatDefinition(is, header);

		header.samFileHeader = readSAMFileHeader(new String(header.id), is);
		return header;
	}

	public static int writeContainer(Container c, OutputStream os) throws IOException {

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
			c.blockCount++;
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

	public static Container readContainer(InputStream is) throws IOException {
		return readContainer(is, 0, Integer.MAX_VALUE);
	}

	public static Container readContainerHeader(InputStream is) throws IOException {
		Container c = new Container();
		ContainerHeaderIO chio = new ContainerHeaderIO();
		if (!chio.readContainerHeader(c, is)) {
			log.info("No more data, must be the end of stream.");
			return null;
		}
		if (c.isEOF()) {
			log.info("Found poison container, assuming end of stream.");
			return null;
		}
		return c;
	}

	public static Container readContainer(InputStream is, int fromSlice, int howManySlices) throws IOException {

		long time1 = System.nanoTime();
		Container c = readContainerHeader(is);
		if (c == null)
			return null;

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
			slices.add(slice);
		}

		c.slices = slices.toArray(new Slice[slices.size()]);

		calculateSliceOffsetsAndSizes(c);

		long time2 = System.nanoTime();

		log.debug("READ CONTAINER: " + c.toString());
		c.readTime = time2 - time1;

		return c;
	}

	private static void calculateSliceOffsetsAndSizes(Container c) {
		if (c.slices.length == 0)
			return;
		for (int i = 0; i < c.slices.length - 1; i++) {
			Slice s = c.slices[i];
			s.offset = c.landmarks[i];
			s.size = c.landmarks[i + 1] - s.offset;
		}
		Slice lastSlice = c.slices[c.slices.length - 1];
		lastSlice.offset = c.landmarks[c.landmarks.length - 1];
		lastSlice.size = c.containerByteSize - lastSlice.offset;
	}

	public static byte[] toByteArray(SAMFileHeader samFileHeader) {
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

		ByteArrayOutputStream headerOS = new ByteArrayOutputStream();
		try {
			headerOS.write(bytes);
			headerOS.write(headerBodyOS.getBuffer(), 0, headerBodyOS.size());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return headerOS.toByteArray();
	}

	private static long writeContainerForSamFileHeader(SAMFileHeader samFileHeader, OutputStream os) throws IOException {
		byte[] data = toByteArray(samFileHeader);
		return writeContainerForSamFileHeaderData(data, 0, Math.max(1024, data.length + data.length / 2), os);
	}

	private static long writeContainerForSamFileHeaderData(byte[] data, int offset, int len, OutputStream os)
			throws IOException {
		Block block = new Block();
		byte[] blockContent = new byte[len];
		System.arraycopy(data, 0, blockContent, offset, Math.min(data.length - offset, len));
		block.setRawContent(blockContent);
		block.method = BlockCompressionMethod.RAW;
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
		int containerHeaderByteSize = chio.writeContainerHeader(c, os);
		os.write(baos.getBuffer(), 0, baos.size());

		return containerHeaderByteSize + baos.size();
	}

	public static SAMFileHeader readSAMFileHeader(String id, InputStream is) throws IOException {
		Container container = readContainerHeader(is);
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

		BufferedLineReader r = new BufferedLineReader(new ByteArrayInputStream(bytes));
		return new SAMTextHeaderCodec().decode(r, id);
	}

	public static boolean replaceCramHeader(File file, CramHeader newHeader) throws IOException {

		int MAP_SIZE = (int) Math.min(1024 * 1024, file.length());
		FileInputStream inputStream = new FileInputStream(file);
		CountingInputStream cis = new CountingInputStream(inputStream);

		CramHeader header = new CramHeader();
		readFormatDefinition(cis, header);

		if (header.majorVersion != newHeader.majorVersion && header.minorVersion != newHeader.minorVersion) {
			log.error(String.format("Cannot replace CRAM header because format versions differ: ", header.majorVersion,
					header.minorVersion, newHeader.majorVersion, header.minorVersion, file.getAbsolutePath()));
			cis.close();
			return false;
		}

		readContainerHeader(cis);
		Block b = new Block(cis, false, false);
		long dataStart = cis.getCount();
		cis.close();

		byte[] data = toByteArray(newHeader.samFileHeader);

		if (data.length > b.getRawContentSize()) {
			log.error("Failed to replace CRAM header because the new header is bigger.");
			return false;
		}

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel channelOut = raf.getChannel();
		MappedByteBuffer mapOut = channelOut.map(MapMode.READ_WRITE, dataStart, MAP_SIZE - dataStart);
		mapOut.put(data);
		mapOut.force();

		channelOut.close();
		raf.close();

		return true;
	}

}
