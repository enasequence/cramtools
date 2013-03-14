package net.sf.cram.structure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.cram.io.ByteBufferUtils;

public class ContainerHeaderIO {

	public void readContainerHeader(Container c, InputStream is)
			throws IOException {
		c.containerByteSize = ByteBufferUtils.int32(is);
		c.sequenceId = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentStart = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentSpan = ByteBufferUtils.readUnsignedITF8(is);
		c.nofRecords = ByteBufferUtils.readUnsignedITF8(is);
		c.globalRecordCounter = ByteBufferUtils.readUnsignedITF8(is);
		c.bases = ByteBufferUtils.readUnsignedITF8(is);
		c.blockCount = ByteBufferUtils.readUnsignedITF8(is);
		c.landmarks = ByteBufferUtils.array(is);
	}
	
	public int writeContainerHeader(Container c, OutputStream os)
			throws IOException {
		int len = ByteBufferUtils.writeInt32(c.containerByteSize, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.sequenceId, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.alignmentStart, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.alignmentSpan, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.nofRecords, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.globalRecordCounter, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.bases, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.blockCount, os);
		len += ByteBufferUtils.write(c.landmarks, os);

		return len;
	}
}
