package net.sf.cram.structure;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import net.sf.cram.build.CramIO;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.samtools.SAMFileHeader;

import org.junit.Test;

public class TestEOFMarker {

	@Test
	public void testbytesFromHex() {
		assertThat(ByteBufferUtils.bytesFromHex("00"), is(new byte[] { 0 }));
		assertThat(ByteBufferUtils.bytesFromHex("01"), is(new byte[] { 1 }));
		assertThat(ByteBufferUtils.bytesFromHex("0f"), is(new byte[] { 15 }));
		assertThat(ByteBufferUtils.bytesFromHex("f0"), is(new byte[] { -16 }));
		assertThat(ByteBufferUtils.bytesFromHex("ff"), is(new byte[] { (byte) 255 }));

		assertThat(ByteBufferUtils.bytesFromHex("00 00"), is(new byte[] { 0, 0 }));
		assertThat(ByteBufferUtils.bytesFromHex("00 01"), is(new byte[] { 0, 1 }));
		assertThat(ByteBufferUtils.bytesFromHex("00 01 f0"), is(new byte[] { 0, 1, -16 }));
		assertThat(ByteBufferUtils.bytesFromHex("00 01 f0 ff"), is(new byte[] { 0, 1, -16, -1 }));
	}

	@Test
	public void testZeroB_EOF_marker() throws IOException {
		Container container;
		container = CramIO.readContainer(new ByteArrayInputStream(CramIO.ZERO_B_EOF_MARKER));
		assertNull(container);
	}

	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testZeroZero_EOF_marker() throws IOException {
		Container container;
		container = CramIO.readContainer(new ByteArrayInputStream(ByteBufferUtils
				.bytesFromHex("00 00 00 00 e0 45 4f 46 00 00 00 00 00 00 00")));
		assertNull(container);
	}

	@Test
	public void testCramIO_ZeroB() throws IOException {
		SAMFileHeader samFileHeader = new SAMFileHeader();
		CramHeader cramHeader = new CramHeader(2, 0, "test", samFileHeader);
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		CramIO.writeCramHeader(cramHeader, baos);
		CramIO.issueZeroB_EOF_marker(baos);
		baos.close();
		byte[] data = baos.toByteArray();

		File temp = File.createTempFile("test", ".cram");
		temp.deleteOnExit();

		FileOutputStream fos = new FileOutputStream(temp);
		fos.write(data);
		fos.close();

		assertThat(CramIO.hasZeroB_EOF_marker(temp), is(true));

		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		CramHeader cramHeader2 = CramIO.readCramHeader(bais);
		assertThat(cramHeader2, equalTo(cramHeader));

		Container c = CramIO.readContainer(bais);
		assertNull(c);

		bais = new ByteArrayInputStream(data);
		CramIO.readCramHeader(bais);
		ContainerHeaderIO chio = new ContainerHeaderIO();
		c = new Container();
		assertThat(chio.readContainerHeader(c, bais), is(true));
		assertTrue(c.isEOF());

		assertThat(c.containerByteSize, is(11));
		assertThat(c.sequenceId, is(-1));
		assertThat(c.alignmentStart, is(4542278));
		assertThat(c.alignmentSpan, is(0));
		assertThat(c.bases, is(0L));
		assertThat(c.blockCount, is(1));

		CompressionHeaderBLock chb = new CompressionHeaderBLock(2, bais);
		c.h = chb.getCompressionHeader();

		assertThat(c.h, notNullValue());

	}
}
