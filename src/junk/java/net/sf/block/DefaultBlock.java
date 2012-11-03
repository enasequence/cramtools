package net.sf.block;

import java.nio.ByteBuffer;

public class DefaultBlock {

	public CompressionMethod compressionMethod;
	public byte contentType;
	public int contentId;
	public int bytes;
	public int dataBytes;
	protected byte[] data;

	public DefaultBlock() {
	}

	public void write(ByteBuffer buf) {

	}

	public void read(ByteBuffer buf) {
		
	}
}
