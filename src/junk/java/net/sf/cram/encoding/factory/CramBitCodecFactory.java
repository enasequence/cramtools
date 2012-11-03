package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.encoding.BitCodec;

public interface CramBitCodecFactory<T> {

	public BitCodec<T> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException,
			CramException;

	public BitCodecStats getStatistics(DefaultMutableTreeNode node);
}
