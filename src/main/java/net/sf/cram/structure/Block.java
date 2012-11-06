package net.sf.cram.structure;

import java.util.Arrays;


public class Block {
	public BlockContentType contentType;
	public byte[] content;
	public int contentId;
	public int method;
	public int rawContentSize ;
	public int compressedContentSize ;

	@Override
	public String toString() {
		return String.format(
				"method=%d, type=%s, id=%d, raw=%d, compressed=%d, content=%s.",
				method, contentType.name(), contentId, rawContentSize, compressedContentSize, 
				Arrays.toString(Arrays.copyOf(content, 20)));
	}
}