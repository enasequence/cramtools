package net.sf.cram.encoding;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import net.sf.cram.CramRecord;
import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingKey;
import net.sf.cram.ReadTag;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.RefSkip;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;

public class Writer {

	public static final int TEST_MARK = 0xA0B0C0D0;
	public Charset charset = Charset.forName("UTF8");
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
	
	@DataSeries(key = EncodingKey.QS_QualityScore, type = DataSeriesType.BYTE_ARRAY)
	public DataWriter<byte[]> qcArray;

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
	
	@DataSeries(key = EncodingKey.TL_TagIdList, type = DataSeriesType.INT)
	public DataWriter<Integer> tagIdListCodec;
	
	@DataSeries(key = EncodingKey.RI_RefId, type = DataSeriesType.INT)
	public DataWriter<Integer> refIdCodec;
	
	@DataSeries(key = EncodingKey.RS_RefSkip, type = DataSeriesType.INT)
	public DataWriter<Integer> refSkipCodec;
	
	public static int detachedCount = 0 ;

	public void write(CramRecord r) throws IOException {
//		testC.writeData(TEST_MARK) ;
		
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
		tagIdListCodec.writeData(r.tagIdsIndex.value) ;
		if (r.tags != null) {
			for (int i=0; i<r.tags.length; i++) {
				DataWriter<byte[]> writer = tagValueCodecs.get(r.tags[i].keyType3BytesAsInt);
				writer.writeData(r.tags[i].getValueAsByteArray());
			}
		}

		if (!r.segmentUnmapped) {
			// writing read features:
			nfc.writeData(r.getReadFeatures().size());
			int prevPos = 0;
			for (ReadFeature f : r.getReadFeatures()) {
				fc.writeData(f.getOperator());
				switch (f.getOperator()) {
				case Substitution.operator:
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
				case Substitution.operator:
					Substitution sv = (Substitution) f;
					bsc.writeData((byte) sv.getBaseChange().getChange());
					break;
				case Insertion.operator:
					Insertion iv = (Insertion) f;
					inc.writeData(iv.getSequence());
					break;
				case SoftClip.operator:
					SoftClip fv = (SoftClip) f;
					inc.writeData(fv.getSequence());
					break;
				case Deletion.operator:
					Deletion dv = (Deletion) f;
					dlc.writeData(dv.getLength());
					break;
				case RefSkip.operator:
					RefSkip rsv = (RefSkip) f;
					refSkipCodec.writeData(rsv.getLength());
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
				qcArray.writeData(r.getQualityScores()) ;
			}
		} else {
			for (byte b : r.getReadBases())
				bc.writeData(b);
			if (r.forcePreserveQualityScores) {
				qcArray.writeData(r.getQualityScores()) ;
			}
		}
	}
}
