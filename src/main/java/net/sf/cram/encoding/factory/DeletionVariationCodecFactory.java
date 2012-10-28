package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.DeletionVariationCodec;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;

class DeletionVariationCodecFactory extends
		AbstractCodecFactory<DeletionVariation> {

	public DeletionVariationCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<DeletionVariation> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {
		DeletionVariationCodec deletionCodec = new DeletionVariationCodec();
		DefaultMutableTreeNode delNode = buildNode(deletionCodec,
				"Deletion codec");
		parent.add(delNode);

		deletionCodec.dellengthPosCodec = register(
				bitCodecFactory.buildLongCodec(compression
						.eMap.get(EncodingKey.DL_DeletionLength)), delNode,
				"Deletion length codec");

		return deletionCodec;
	}

}
