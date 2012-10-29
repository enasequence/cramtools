package net.sf.cram.encoding.factory;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.CramRecord;
import net.sf.cram.CramRecordCodec;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;

public class RecordCodecFactory extends
		AbstractCodecFactory<CramRecord> {
	private ReadFeatureCodecFactory readFeatureCodecFactory;
	private ReadTagCollectionCodecFactory readTagCollectionFactory;

	public RecordCodecFactory(boolean collectStats,
			BitCodecFactory bitCodecFactory) {
		super(collectStats, bitCodecFactory);
		readFeatureCodecFactory = new ReadFeatureCodecFactory(collectStats,
				bitCodecFactory);
		readTagCollectionFactory = new ReadTagCollectionCodecFactory(
				collectStats, bitCodecFactory);
	}

	@Override
	public BitCodec<CramRecord> buildCodec(
			CramHeader header, CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode root) throws CramException {

		CramRecordCodec recordCodec = new CramRecordCodec();
		if (root == null)
			root = buildNode(recordCodec, "Cram record codec");

		recordCodec.prevPosInSeq = compression.firstRecordPosition;

		recordCodec.inSeqPosCodec = register(
				bitCodecFactory.buildLongCodec(compression.eMap
						.get(EncodingKey.AP_AlignmentPositionOffset)), root,
				"Refpos codec");
		recordCodec.recordsToNextFragmentCodec = register(
				bitCodecFactory.buildLongCodec(compression.eMap
						.get(EncodingKey.NF_RecordsToNextFragment)), root,
				"Rank codec");

		recordCodec.readlengthCodec = register(
				bitCodecFactory.buildLongCodec(compression.eMap
						.get(EncodingKey.RL_ReadLength)), root,
				"Read length codec");

		recordCodec.variationsCodec = readFeatureCodecFactory.buildCodec(
				header, compression, referenceBaseProvider, root);

		recordCodec.readGroupCodec = register(
				bitCodecFactory.buildIntegerCodec(compression.eMap
						.get(EncodingKey.RG_ReadGroup)), root,
				"Read group index codec");

		recordCodec.mappingQualityCodec = register(
				bitCodecFactory.buildByteCodec(compression.eMap
						.get(EncodingKey.MQ_MappingQualityScore)), root,
				"Mapping quality codec");

		recordCodec.storeMappedQualityScores = compression.mappedQualityScoreIncluded;

		recordCodec.baseCodec = register(
				bitCodecFactory.buildByteArrayCodec(compression.eMap
						.get(EncodingKey.BA_Base)), root, "Read bases codec");
		recordCodec.qualityCodec = register(
				bitCodecFactory.buildByteArrayCodec2(compression.eMap
						.get(EncodingKey.QS_QualityScore)), root,
				"Quality score codec");

		recordCodec.readTagCodec = readTagCollectionFactory.buildCodec(header,
				compression, referenceBaseProvider, root);

		recordCodec.flagsCodec = register(
				bitCodecFactory.buildByteCodec(compression.eMap
						.get(EncodingKey.BF_BitFlags)), root,
				"Read flags codec");

		recordCodec.readNameCodec = register(
				bitCodecFactory.buildByteArrayCodec(compression.eMap
						.get(EncodingKey.RN_ReadName)), root, "Read name codec");
		recordCodec.mateAlignemntStartCodec = register(
				bitCodecFactory.buildLongCodec(compression.eMap
						.get(EncodingKey.NP_NextFragmentAlignmentStart)), root,
				"Mate alignment start codec");
		recordCodec.insertSizeCodec = register(
				bitCodecFactory.buildIntegerCodec(compression.eMap
						.get(EncodingKey.TS_InsetSize)), root,
				"Insert size codec");

		return recordCodec;
	}

}
