package net.sf.cram.structure;

import java.util.Arrays;


public class Block {
	public BlockContentType contentType;
	public byte[] content;
	public int contentId;
	public int method;

	@Override
	public String toString() {
		return String.format(
				"BLOCK: method=%d, type=%s, id=%d, len=%d, content=%s.",
				method, contentType.name(), contentId, content.length,
				Arrays.toString(Arrays.copyOf(content, 20)));
	}
}