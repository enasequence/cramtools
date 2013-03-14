package net.sf.cram.structure;

import java.io.IOException;
import java.io.InputStream;

public class CompressionHeaderBLock extends Block {
	private CompressionHeader compressionHeader;

	public CompressionHeader getCompressionHeader() {
		return compressionHeader;
	}

	public CompressionHeaderBLock(CompressionHeader header) {
		super();
		this.compressionHeader = header;
		contentType = BlockContentType.COMPRESSION_HEADER;
		contentId = 0;
		method = BlockCompressionMethod.RAW.ordinal();
		byte[] bytes;
		try {
			bytes = header.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("This should have never happend.");
		}
		setRawContent(bytes);
	}

	public CompressionHeaderBLock(InputStream is) throws IOException {
		super(is, true, true);

		if (contentType != BlockContentType.COMPRESSION_HEADER)
			throw new RuntimeException("Content type does not match: "
					+ contentType.name());

		compressionHeader = new CompressionHeader();
		compressionHeader.read(getRawContent());
	}
}
