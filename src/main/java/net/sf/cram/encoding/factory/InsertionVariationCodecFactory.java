package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.InsertionVariationCodec;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;

class InsertionVariationCodecFactory extends
		AbstractCodecFactory<InsertionVariation> {

	public InsertionVariationCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<InsertionVariation> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {

		InsertionVariationCodec codec = new InsertionVariationCodec();
		DefaultMutableTreeNode node = buildNode(codec, "Insertion codec");
		parent.add(node);

		codec.insertBasesCodec = register(
				bitCodecFactory.buildByteArrayCodec(compression
						.eMap.get(EncodingKey.BA_Base)), node,
				"Insertion bases codec");

		return codec;
	}

}
