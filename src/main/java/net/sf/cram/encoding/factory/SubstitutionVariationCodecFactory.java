package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.BaseChangeCodec;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.cram.encoding.read_features.SubstitutionVariationCodec;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;

class SubstitutionVariationCodecFactory extends
		AbstractCodecFactory<SubstitutionVariation> {

	public SubstitutionVariationCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<SubstitutionVariation> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {
		SubstitutionVariationCodec substitutionCodec = new SubstitutionVariationCodec();
		DefaultMutableTreeNode delNode = buildNode(substitutionCodec,
				"Substitution codec");
		parent.add(delNode);

		substitutionCodec.baseChangeCodec = register(new BaseChangeCodec(),
				delNode, "Base change codec");

		return substitutionCodec;
	}

}
