package net.sf.cram.encoding;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import net.sf.cram.CramRecord;
import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingKey;
import net.sf.cram.ReadTag;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SoftClipVariation;
import net.sf.cram.encoding.read_features.SubstitutionVariation;

public class Writer {

	public static final int TEST_MARK = 0xF0F0F0;
	public Charset charset = Charset.forName("UTF8");
	public boolean captureMappedQS = false;
	public boolean captureUnmappedQS = false;
	public boolean captureReadNames = false;
	
	@DataSeries(key = EncodingKey.BF_BitFlags, type = DataSeriesType.INT)
	public DataWriter<Integer> bitFlagsC;
	
	@DataSeries(key = EncodingKey.CF_CompressionBitFlags, type = DataSeriesType.BYTE)
	public DataWriter<Byte> compBitFlagsC;

	@DataSeries(key = EncodingKey.RL_ReadLength, type = DataSeriesType.INT)
	public DataWriter<Integer> readLengthC;

	@DataSeries(key = EncodingKey.AP_AlignmentPositionOffset, type = DataSeriesType.INT)
	public DataWriter<Integer> alStartC;

	@DataSeries(key = EncodingKey.RG_ReadGroup, type = DataSeriesType.INT)
	public DataWriter<Integer> readGroupC;

	@DataSeries(key = EncodingKey.RN_ReadName, type = DataSeriesType.BYTE_ARRAY)
	public DataWriter<byte[]> readNameC;

	@DataSeries(key = EncodingKey.NF_RecordsToNextFragment, type = DataSeriesType.INT)
	public DataWriter<Integer> distanceC;

	@DataSeries(key = EncodingKey.TC_TagCount, type = DataSeriesType.BYTE)
	public DataWriter<Byte> tagCountC;

	@DataSeries(key = EncodingKey.TN_TagNameAndType, type = DataSeriesType.INT)
	public DataWriter<Integer> tagNameAndTypeC;

	@DataSeriesMap(name = "TAG")
	public Map<Integer, DataWriter<byte[]>> tagValueCodecs;

	@DataSeries(key = EncodingKey.FN_NumberOfReadFeatures, type = DataSeriesType.INT)
	public DataWriter<Integer> nfc;

	@DataSeries(key = EncodingKey.FP_FeaturePosition, type = DataSeriesType.INT)
	public DataWriter<Integer> fp;

	@DataSeries(key = EncodingKey.FC_FeatureCode, type = DataSeriesType.BYTE)
	public DataWriter<Byte> fc;

	@DataSeries(key = EncodingKey.BA_Base, type = DataSeriesType.BYTE)
	public DataWriter<Byte> bc;

	@DataSeries(key = EncodingKey.QS_QualityScore, type = DataSeriesType.BYTE)
	public DataWriter<Byte> qc;

	@DataSeries(key = EncodingKey.BS_BaseSubstitutionCode, type = DataSeriesType.BYTE)
	public DataWriter<Byte> bsc;

	@DataSeries(key = EncodingKey.IN_Insertion, type = DataSeriesType.BYTE_ARRAY)
	public DataWriter<byte[]> inc;

	@DataSeries(key = EncodingKey.DL_DeletionLength, type = DataSeriesType.INT)
	public DataWriter<Integer> dlc;

	@DataSeries(key = EncodingKey.MQ_MappingQualityScore, type = DataSeriesType.INT)
	public DataWriter<Integer> mqc;

	@DataSeries(key = EncodingKey.MF_MateBitFlags, type = DataSeriesType.BYTE)
	public DataWriter<Byte> mbfc;

	@DataSeries(key = EncodingKey.NS_NextFragmentReferenceSequenceID, type = DataSeriesType.INT)
	public DataWriter<Integer> mrc;

	@DataSeries(key = EncodingKey.NP_NextFragmentAlignmentStart, type = DataSeriesType.INT)
	public DataWriter<Integer> malsc;

	@DataSeries(key = EncodingKey.TS_InsetSize, type = DataSeriesType.INT)
	public DataWriter<Integer> tsc;
	

	@DataSeries(key = EncodingKey.TM_TestMark, type = DataSeriesType.INT)
	public DataWriter<Integer> testC;
	
	public static int detachedCount = 0 ;

	public void write(CramRecord r) throws IOException {
		bitFlagsC.writeData(r.getFlags());
		compBitFlagsC.writeData(r.getCompressionFlags()) ;
		
		readLengthC.writeData(r.getReadLength());
		alStartC.writeData(r.alignmentStartOffsetFromPreviousRecord);
		readGroupC.writeData(r.getReadGroupID());

		if (captureReadNames) {
			readNameC.writeData(r.getReadName().getBytes(charset));
		}

		// mate record:
		if (r.detached) {
			mbfc.writeData(r.getMateFlags());
			if (!captureReadNames)
				readNameC.writeData(r.getReadName().getBytes(charset));

			mrc.writeData(r.mateSequnceID);
			malsc.writeData(r.mateAlignmentStart);
			tsc.writeData(r.templateSize);
			
			detachedCount++ ;
		} else if (r.hasMateDownStream) 
			distanceC.writeData(r.recordsToNextFragment);

		// tag records:
		tagCountC.writeData(r.tags == null ? 0 : (byte) r.tags.size());
		if (r.tags != null) {
			for (ReadTag tag : r.tags) {
				tagNameAndTypeC.writeData(tag.keyType3BytesAsInt);

				DataWriter<byte[]> writer = tagValueCodecs.get(tag.keyType3BytesAsInt);
				writer.writeData(tag.getValueAsByteArray());
			}
		}

		if (!r.segmentUnmapped) {
			// writing read features:
			nfc.writeData(r.getReadFeatures().size());
			int prevPos = 0;
			for (ReadFeature f : r.getReadFeatures()) {
				fc.writeData(f.getOperator());
				switch (f.getOperator()) {
				case SubstitutionVariation.operator:
					break;

				default:
					break;
				}

				fp.writeData(f.getPosition() - prevPos);
				prevPos = f.getPosition();

				switch (f.getOperator()) {
				case ReadBase.operator:
					ReadBase rb = (ReadBase) f;
					bc.writeData(rb.getBase());
					qc.writeData(rb.getQualityScore());
					break;
				case SubstitutionVariation.operator:
					SubstitutionVariation sv = (SubstitutionVariation) f;
					bsc.writeData((byte) sv.getBaseChange().getChange());
					break;
				case InsertionVariation.operator:
					InsertionVariation iv = (InsertionVariation) f;
					inc.writeData(iv.getSequence());
					break;
				case SoftClipVariation.operator:
					SoftClipVariation fv = (SoftClipVariation) f;
					inc.writeData(fv.getSequence());
					break;
				case DeletionVariation.operator:
					DeletionVariation dv = (DeletionVariation) f;
					dlc.writeData(dv.getLength());
					break;
				case InsertBase.operator:
					InsertBase ib = (InsertBase) f;
					bc.writeData(ib.getBase());
					break;
				case BaseQualityScore.operator:
					BaseQualityScore bqs = (BaseQualityScore) f;
					qc.writeData(bqs.getQualityScore());
					break;
				default:
					throw new RuntimeException(
							"Unknown read feature operator: "
									+ (char) f.getOperator());
				}
			}

			// mapping quality:
			mqc.writeData(r.getMappingQuality());
			if (r.forcePreserveQualityScores) {
				for (byte q : r.getQualityScores())
					qc.writeData(q);
			}
		} else {
			for (byte b : r.getReadBases())
				bc.writeData(b);
			if (r.forcePreserveQualityScores) {
				for (byte q : r.getQualityScores())
					qc.writeData(q);
			}
		}

//		testC.writeData(TEST_MARK) ;
	}
}
