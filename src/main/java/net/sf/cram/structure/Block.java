package net.sf.cram.structure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import net.sf.cram.io.ByteBufferUtils;

public class Block {
	// TODO change int method to BlockCompressionMethod enum:
	public int method;
	public BlockContentType contentType;
	public int contentId;
	private int compressedContentSize;
	private int rawContentSize;
	// public byte[] content;

	private byte[] rawContent, compressedContent;

	public Block() {
	}

	public Block(int method, BlockContentType contentType, int contentId,
			byte[] rawContent, byte[] compressedContent) {
		this.method = method;
		this.contentType = contentType;
		this.contentId = contentId;
		if (rawContent != null)
			setRawContent(rawContent);
		if (compressedContent != null)
			setCompressedContent(compressedContent);
	}

	public Block(InputStream is, boolean readContent, boolean uncompress)
			throws IOException {
		method = is.read();

		int contentTypeId = is.read();
		contentType = BlockContentType.values()[contentTypeId];

		contentId = ByteBufferUtils.readUnsignedITF8(is);
		compressedContentSize = ByteBufferUtils.readUnsignedITF8(is);
		rawContentSize = ByteBufferUtils.readUnsignedITF8(is);

		if (readContent) {
			compressedContent = new byte[compressedContentSize];
			ByteBufferUtils.readFully(compressedContent, is);

			if (uncompress)
				uncompress();
		}
	}

	@Override
	public String toString() {
		String raw = rawContent == null ? "NULL" : Arrays.toString(Arrays
				.copyOf(rawContent, 20));
		String comp = compressedContent == null ? "NULL" : Arrays
				.toString(Arrays.copyOf(compressedContent, 20));

		return String
				.format("method=%d, type=%s, id=%d, raw size=%d, compressed size=%d, raw=%s, comp=%s.",
						method, contentType.name(), contentId, rawContentSize,
						compressedContentSize, raw, comp);
	}

	public boolean isCompressed() {
		return compressedContent != null;
	}

	public boolean isUncompressed() {
		return rawContent != null;
	}

	public void setRawContent(byte[] raw) {
		rawContent = raw;
		rawContentSize = raw == null ? 0 : raw.length;

		compressedContent = null;
		compressedContentSize = 0;
	}

	public byte[] getRawContent() {
		if (rawContent == null)
			uncompress();
		return rawContent;
	}

	public void setCompressedContent(byte[] compressed) {
		this.compressedContent = compressed;
		compressedContentSize = compressed == null ? 0 : compressed.length;

		rawContent = null;
		rawContentSize = 0;
	}

	public byte[] getCompressedContent() {
		if (compressedContent == null)
			compress();
		return compressedContent;
	}

	public void compress() {
		if (compressedContent != null || rawContent == null)
			return;

		BlockCompressionMethod m = BlockCompressionMethod.values()[method];

		switch (m) {
		case RAW:
			compressedContent = rawContent;
			compressedContentSize = rawContentSize;
			break;
		case GZIP:
			try {
				compressedContent = ByteBufferUtils.gzip(rawContent);
			} catch (IOException e) {
				throw new RuntimeException("This should have never happned.", e);
			}
			compressedContentSize = compressedContent.length;
			break;
		default:
			break;
		}
	}

	public void uncompress() {
		if (rawContent != null || compressedContent == null)
			return;

		BlockCompressionMethod m = BlockCompressionMethod.values()[method];

		switch (m) {
		case RAW:
			rawContent = compressedContent;
			rawContentSize = compressedContentSize;
			break;
		case GZIP:
			try {
				rawContent = ByteBufferUtils.gunzip(compressedContent);
			} catch (IOException e) {
				throw new RuntimeException("This should have never happned.", e);
			}
			break;
		default:
			throw new RuntimeException("Unknown block compression method: "
					+ m.name());
		}
	}

	public void write(OutputStream os) throws IOException {
		if (!isCompressed())
			compress();
		if (!isUncompressed())
			uncompress();

		os.write(method);
		os.write(contentType.ordinal());
		os.write(contentId);

		ByteBufferUtils.writeUnsignedITF8(compressedContentSize, os);
		ByteBufferUtils.writeUnsignedITF8(rawContentSize, os);

		os.write(getCompressedContent());
	}
}