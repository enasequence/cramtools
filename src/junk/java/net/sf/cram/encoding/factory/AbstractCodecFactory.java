package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.MeasuringCodec;

abstract class AbstractCodecFactory<T> implements CramBitCodecFactory<T> {

	private boolean useMeasuringCodecWrapper = true;
	protected BitCodecFactory bitCodecFactory;

	public AbstractCodecFactory(boolean useMeasuringCodecWrapper, BitCodecFactory bitCodecFactory) {
		this.useMeasuringCodecWrapper = useMeasuringCodecWrapper;
		this.bitCodecFactory = bitCodecFactory;
	}

	@Override
	public BitCodecStats getStatistics(DefaultMutableTreeNode node) {
		Object codec = node.getUserObject();
		BitCodecStats stats = new BitCodecStats();
		if (codec instanceof MeasuringCodec<?>) {
			stats.name = ((MeasuringCodec) codec).getName();
			stats.bitsProduced = ((MeasuringCodec) codec).getWrittenBits();
			stats.bitsConsumed = ((MeasuringCodec) codec).getWrittenBits();
			stats.objectsWritten = ((MeasuringCodec) codec).getWrittenObjects();
			stats.objectsRead = ((MeasuringCodec) codec).getReadObjects();

			stats.arraysRead = stats.objectsRead;
			stats.arraysWritten = stats.objectsWritten;
		} else {
			stats.name = ((ByteArrayBitCodec) codec).getName();
			stats.bitsProduced = ((ByteArrayBitCodec) codec).getStats().nofBis;
			stats.bitsConsumed = ((ByteArrayBitCodec) codec).getStats().nofBis;
			stats.objectsWritten = ((ByteArrayBitCodec) codec).getStats().bytesWritten;
			stats.objectsRead = ((ByteArrayBitCodec) codec).getStats().bytesRead;
			stats.arraysWritten = ((ByteArrayBitCodec) codec).getStats().arraysWritten;
			stats.arraysRead = ((ByteArrayBitCodec) codec).getStats().arraysRead;
		}

		return stats;
	}

	protected <T> BitCodec<T> register(BitCodec<T> codec, DefaultMutableTreeNode parent, String name) {
		BitCodec<T> newCodec = useMeasuringCodecWrapper ? new MeasuringCodec<T>(codec, name) : codec;
		if (parent != null)
			parent.add(new DefaultMutableTreeNode(newCodec));
		return newCodec;
	}

	protected ByteArrayBitCodec register(ByteArrayBitCodec codec, DefaultMutableTreeNode parent, String name) {
		ByteArrayBitCodec newCodec = codec;
		if (parent != null)
			parent.add(new DefaultMutableTreeNode(newCodec));
		return newCodec;
	}

	protected <T> DefaultMutableTreeNode buildNode(BitCodec<T> codec, String name) {
		BitCodec<T> newCodec = useMeasuringCodecWrapper ? new MeasuringCodec<T>(codec, name) : codec;
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(newCodec);
		return node;
	}

}
