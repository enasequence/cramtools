package net.sf.cram.structure;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.sf.cram.io.ByteBufferUtils;
import net.sf.samtools.util.IOUtil;

public class ContainerHeaderIO {

	public boolean readContainerHeader(Container c, InputStream is)
			throws IOException {
		byte[] peek = new byte[4] ;
		try {
			ByteBufferUtils.readFully(peek, is) ;
		} catch (EOFException e) {
			return false ;
		}
		
		c.containerByteSize = ByteBufferUtils.int32(peek);
		c.sequenceId = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentStart = ByteBufferUtils.readUnsignedITF8(is);
		c.alignmentSpan = ByteBufferUtils.readUnsignedITF8(is);
		c.nofRecords = ByteBufferUtils.readUnsignedITF8(is);
		c.globalRecordCounter = ByteBufferUtils.readUnsignedLTF8(is);
		c.bases = ByteBufferUtils.readUnsignedLTF8(is);
		c.blockCount = ByteBufferUtils.readUnsignedITF8(is);
		c.landmarks = ByteBufferUtils.array(is);
		
		return true ;
	}
	
	public int writeContainerHeader(Container c, OutputStream os)
			throws IOException {
		int len = ByteBufferUtils.writeInt32(c.containerByteSize, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.sequenceId, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.alignmentStart, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.alignmentSpan, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.nofRecords, os);
		len += ByteBufferUtils.writeUnsignedLTF8(c.globalRecordCounter, os);
		len += ByteBufferUtils.writeUnsignedLTF8(c.bases, os);
		len += ByteBufferUtils.writeUnsignedITF8(c.blockCount, os);
		len += ByteBufferUtils.write(c.landmarks, os);

		return len;
	}
}
