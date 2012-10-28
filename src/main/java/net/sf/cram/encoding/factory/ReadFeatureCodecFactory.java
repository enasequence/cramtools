package net.sf.cram.encoding.factory;

import java.util.List;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.ReadFeatureCodec;
import uk.ac.ebi.ena.sra.cram.CramException;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.ReadFeature;

class ReadFeatureCodecFactory extends AbstractCodecFactory<List<ReadFeature>> {
	private DeletionVariationCodecFactory delFactory;
	private SubstitutionVariationCodecFactory subFactory;
	private InsertionVariationCodecFactory insFactory;
	private InsertBaseCodecFactory insBaseFactory;
	private BaseQualityCodecFactory baseQualityFactory;
	private ReadBaseCodecFactory readBaseFactory;

	public ReadFeatureCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
		delFactory = new DeletionVariationCodecFactory(
				useMeasuringCodecWrapper, bitCodecFactory);
		subFactory = new SubstitutionVariationCodecFactory(
				useMeasuringCodecWrapper, bitCodecFactory);
		insFactory = new InsertionVariationCodecFactory(
				useMeasuringCodecWrapper, bitCodecFactory);
		insBaseFactory = new InsertBaseCodecFactory(useMeasuringCodecWrapper,
				bitCodecFactory);
		baseQualityFactory = new BaseQualityCodecFactory(
				useMeasuringCodecWrapper, bitCodecFactory);
		readBaseFactory = new ReadBaseCodecFactory(useMeasuringCodecWrapper,
				bitCodecFactory);
	}

	@Override
	public BitCodec<List<ReadFeature>> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramException {

		ReadFeatureCodec codec = new ReadFeatureCodec();
		DefaultMutableTreeNode node = buildNode(codec, "Variations codec");
		parent.add(node);

		codec.inReadPosCodec = register(
				bitCodecFactory.buildLongCodec(compression
						.eMap.get(EncodingKey.FP_FeaturePosition)), parent,
				"Position in read");

		codec.featureOperationCodec = register(
				bitCodecFactory.buildByteCodec(compression
						.eMap.get(EncodingKey.RF_ReadFeatureCode)), node,
				"Read feature operators");

		codec.deletionCodec = delFactory.buildCodec(header, compression,
				referenceBaseProvider, node);
		codec.substitutionCodec = subFactory.buildCodec(header, compression,
				referenceBaseProvider, node);
		codec.insertionCodec = insFactory.buildCodec(header, compression,
				referenceBaseProvider, node);
		codec.insertBaseCodec = insBaseFactory.buildCodec(header, compression,
				referenceBaseProvider, node);
		codec.baseQSCodec = baseQualityFactory.buildCodec(header, compression,
				referenceBaseProvider, node);
		codec.readBaseCodec = readBaseFactory.buildCodec(header, compression,
				referenceBaseProvider, node);

		return codec;
	}

}
