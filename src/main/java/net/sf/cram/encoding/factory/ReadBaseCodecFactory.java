package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadBaseCodec;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;

class ReadBaseCodecFactory extends AbstractCodecFactory<ReadBase> {

	public ReadBaseCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<ReadBase> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {

		ReadBaseCodec codec = new ReadBaseCodec();
		DefaultMutableTreeNode node = buildNode(codec, "Read base codec");
		parent.add(node);

		BitCodec<Byte> baseCodec = bitCodecFactory.buildByteCodec(compression
				.eMap.get(EncodingKey.BA_Base));
		BitCodec<Byte> qualityScoreCodec = bitCodecFactory
				.buildByteCodec(compression
						.eMap.get(EncodingKey.QS_QualityScore));

		codec.baseCodec = register(baseCodec, node, "Read base (bases) codec");
		codec.qualityScoreCodec = register(qualityScoreCodec, node,
				"Read base (score) codec");

		return codec;
	}
}
