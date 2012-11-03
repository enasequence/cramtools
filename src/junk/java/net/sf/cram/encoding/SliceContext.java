package net.sf.cram.encoding;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import net.sf.block.Block;
import net.sf.cram.ContentType;

public class SliceContext {
	private Block[] blocks;
	/**
	 * Each element
	 */
	private int[] sliceBlockIndicies;
	/**
	 * Slice counter for current context
	 */
	private int currentSliceIndex;
	/**
	 * Block counter which holds current slice definition
	 */
	private int currentSliceBlockIndex;
	public Slice currentSlice;
	public Map<Short, Block> sliceBlocks = new HashMap<Short, Block>();

	/**
	 * Switch to the next slice if possible.
	 * 
	 * @return true upon success or false if no more slices left.
	 */
	public boolean next() {
		if (currentSliceIndex >= sliceBlockIndicies.length)
			return false;
		switchTo(currentSliceIndex + 1);
		return true;
	}

	public void switchTo(int sliceIndex) {
		currentSliceIndex = sliceIndex;
		currentSliceBlockIndex = sliceBlockIndicies[currentSliceIndex];

		Block sliceBlock = blocks[currentSliceBlockIndex];
		currentSlice = new Slice(sliceBlock);
		sliceBlocks.clear();
		for (int i = 1; i < currentSlice.blockContentIds.length; i++) {
			Block block = blocks[currentSliceBlockIndex + i];
			sliceBlocks.put(block.contentId, block);
		}
	}

	public static class Slice {
		public boolean mapped;

		public int referenceId;
		public long alignmentStart;
		public long alignmentLength;
		public int numberOfRecords;
		public int numberOfBlocks;
		public short[] blockContentIds;
		public short embeddedReferenceBlockContentId;

		public Slice(Block block) {
			mapped = (block.contentType == ContentType.MAP_SLICE
					.getContentType());
			ByteBuffer buf = ByteBuffer.wrap(block.data);

			if (mapped) {
				referenceId = buf.getInt();
				alignmentStart = buf.getLong();
				alignmentLength = buf.getLong();
				numberOfRecords = buf.getInt();
				numberOfBlocks = buf.getInt();
				blockContentIds = new short[numberOfBlocks];
				for (int i = 0; i < numberOfBlocks; i++)
					blockContentIds[i] = buf.getShort();

				embeddedReferenceBlockContentId = buf.getShort();
			} else {
				numberOfRecords = buf.getInt();
				numberOfBlocks = buf.getInt();
				blockContentIds = new short[numberOfBlocks];
				for (int i = 0; i < numberOfBlocks; i++)
					blockContentIds[i] = buf.getShort();
			}
		}

	}
}
