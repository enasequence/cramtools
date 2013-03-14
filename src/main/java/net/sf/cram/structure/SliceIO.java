package net.sf.cram.structure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import net.sf.cram.io.ByteBufferUtils;

public class SliceIO {

	public void readSliceHeadBlock(Slice s, InputStream is) throws IOException {
		s.headerBlock = new Block(is, true, true);
		parseSliceHeaderBlock(s) ;
	}

	public void parseSliceHeaderBlock(Slice s) throws IOException {
		ByteArrayInputStream is = new ByteArrayInputStream(
				s.headerBlock.getRawContent());

		s.sequenceId = ByteBufferUtils.readUnsignedITF8(is);
		s.alignmentStart = ByteBufferUtils.readUnsignedITF8(is);
		s.alignmentSpan = ByteBufferUtils.readUnsignedITF8(is);
		s.nofRecords = ByteBufferUtils.readUnsignedITF8(is);
		s.globalRecordCounter = ByteBufferUtils.readUnsignedITF8(is);
		s.nofBlocks = ByteBufferUtils.readUnsignedITF8(is);
		s.contentIDs = ByteBufferUtils.array(is);
		s.embeddedRefBlockContentID = ByteBufferUtils.readUnsignedITF8(is);
		s.refMD5 = new byte[16];
		ByteBufferUtils.readFully(s.refMD5, is);
	}

	public byte[] createSliceHeaderBlockContent(Slice s) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteBufferUtils.writeUnsignedITF8(s.sequenceId, baos);
		ByteBufferUtils.writeUnsignedITF8(s.alignmentStart, baos);
		ByteBufferUtils.writeUnsignedITF8(s.alignmentSpan, baos);
		ByteBufferUtils.writeUnsignedITF8(s.nofRecords, baos);
		ByteBufferUtils.writeUnsignedLTF8(s.globalRecordCounter, baos);
		ByteBufferUtils.writeUnsignedITF8(s.nofBlocks, baos);
		ByteBufferUtils.write(s.contentIDs, baos);
		ByteBufferUtils.writeUnsignedITF8(s.embeddedRefBlockContentID, baos);
		baos.write(s.refMD5);
		ByteBufferUtils.writeUnsignedITF8(s.sequenceId, baos);
		ByteBufferUtils.writeUnsignedITF8(s.sequenceId, baos);
		ByteBufferUtils.writeUnsignedITF8(s.sequenceId, baos);

		return baos.toByteArray();
	}

	public void createSliceHeaderBlock(Slice s) throws IOException {
		byte[] rawContent = createSliceHeaderBlockContent(s);
		s.headerBlock = new Block(BlockCompressionMethod.RAW.ordinal(),
				BlockContentType.MAPPED_SLICE, 0, rawContent, null);
	}

	public void readSliceBlocks(Slice s, boolean uncompressBlocks,
			InputStream is) throws IOException {
		s.external = new HashMap<Integer, Block>();
		for (int i = 0; i < s.nofBlocks; i++) {
			Block b1 = new Block(is, true, uncompressBlocks);

			switch (b1.contentType) {
			case CORE:
				s.coreBlock = b1;
				s.external.put(b1.contentId, b1);
				break;
			case EXTERNAL:
				if (s.embeddedRefBlockContentID == b1.contentId)
					s.embeddedRefBlock = b1;
				else
					s.external.put(b1.contentId, b1);
				break;

			default:
				throw new RuntimeException(
						"Not a slice block, content type id "
								+ b1.contentType.name());
			}
		}
	}
}
