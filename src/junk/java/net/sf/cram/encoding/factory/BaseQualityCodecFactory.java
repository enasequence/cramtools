package net.sf.cram.encoding.factory;

import java.io.IOException;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

class BaseQualityCodecFactory extends AbstractCodecFactory<BaseQualityScore> {

	public BaseQualityCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<BaseQualityScore> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {

		final BitCodec<Byte> qualityScoreCodec = bitCodecFactory
				.buildByteCodec(compression.eMap.get(EncodingKey.QS_QualityScore));

		BitCodec<BaseQualityScore> codec = new BitCodec<BaseQualityScore>() {

			@Override
			public BaseQualityScore read(BitInputStream bis) throws IOException {
				BaseQualityScore bqs = new BaseQualityScore(-1,
						qualityScoreCodec.read(bis));
				return bqs;
			}

			@Override
			public long write(BitOutputStream bos, BaseQualityScore bqs)
					throws IOException {
				return qualityScoreCodec.write(bos, bqs.getQualityScore());
			}

			@Override
			public long numberOfBits(BaseQualityScore bqs) {
				return qualityScoreCodec.numberOfBits(bqs.getQualityScore());
			}
		};

		DefaultMutableTreeNode node = buildNode(codec, "Base QS codec");
		parent.add(node);

		return codec;
	}
}
