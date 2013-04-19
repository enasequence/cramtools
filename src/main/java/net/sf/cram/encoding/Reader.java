package net.sf.cram.encoding;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.Map;

import net.sf.cram.CramRecord;
import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingKey;
import net.sf.cram.ReadTag;
import net.sf.cram.encoding.read_features.BaseChange;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.RefSkip;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.structure.SubstitutionMatrix;

public class Reader {
	public Charset charset = Charset.forName("UTF8");
	public boolean captureReadNames = false;
	public byte[][][] tagIdDictionary;

	@DataSeries(key = EncodingKey.BF_BitFlags, type = DataSeriesType.INT)
	public DataReader<Integer> bitFlagsC;

	@DataSeries(key = EncodingKey.CF_CompressionBitFlags, type = DataSeriesType.BYTE)
	public DataReader<Byte> compBitFlagsC;

	@DataSeries(key = EncodingKey.RL_ReadLength, type = DataSeriesType.INT)
	public DataReader<Integer> readLengthC;

	@DataSeries(key = EncodingKey.AP_AlignmentPositionOffset, type = DataSeriesType.INT)
	public DataReader<Integer> alStartC;

	@DataSeries(key = EncodingKey.RG_ReadGroup, type = DataSeriesType.INT)
	public DataReader<Integer> readGroupC;

	@DataSeries(key = EncodingKey.RN_ReadName, type = DataSeriesType.BYTE_ARRAY)
	public DataReader<byte[]> readNameC;

	@DataSeries(key = EncodingKey.NF_RecordsToNextFragment, type = DataSeriesType.INT)
	public DataReader<Integer> distanceC;

	@DataSeries(key = EncodingKey.TC_TagCount, type = DataSeriesType.BYTE)
	public DataReader<Byte> tagCountC;

	@DataSeries(key = EncodingKey.TN_TagNameAndType, type = DataSeriesType.INT)
	public DataReader<Integer> tagNameAndTypeC;

	@DataSeriesMap(name = "TAG")
	public Map<Integer, DataReader<byte[]>> tagValueCodecs;

	@DataSeries(key = EncodingKey.FN_NumberOfReadFeatures, type = DataSeriesType.INT)
	public DataReader<Integer> nfc;

	@DataSeries(key = EncodingKey.FP_FeaturePosition, type = DataSeriesType.INT)
	public DataReader<Integer> fp;

	@DataSeries(key = EncodingKey.FC_FeatureCode, type = DataSeriesType.BYTE)
	public DataReader<Byte> fc;

	@DataSeries(key = EncodingKey.BA_Base, type = DataSeriesType.BYTE)
	public DataReader<Byte> bc;

	@DataSeries(key = EncodingKey.QS_QualityScore, type = DataSeriesType.BYTE)
	public DataReader<Byte> qc;

	@DataSeries(key = EncodingKey.QS_QualityScore, type = DataSeriesType.BYTE_ARRAY)
	public DataReader<byte[]> qcArray;

	@DataSeries(key = EncodingKey.BS_BaseSubstitutionCode, type = DataSeriesType.BYTE)
	public DataReader<Byte> bsc;

	@DataSeries(key = EncodingKey.IN_Insertion, type = DataSeriesType.BYTE_ARRAY)
	public DataReader<byte[]> inc;

	@DataSeries(key = EncodingKey.SC_SoftClip, type = DataSeriesType.BYTE_ARRAY)
	public DataReader<byte[]> softClipCodec;

	@DataSeries(key = EncodingKey.DL_DeletionLength, type = DataSeriesType.INT)
	public DataReader<Integer> dlc;

	@DataSeries(key = EncodingKey.MQ_MappingQualityScore, type = DataSeriesType.INT)
	public DataReader<Integer> mqc;

	@DataSeries(key = EncodingKey.MF_MateBitFlags, type = DataSeriesType.BYTE)
	public DataReader<Byte> mbfc;

	@DataSeries(key = EncodingKey.NS_NextFragmentReferenceSequenceID, type = DataSeriesType.INT)
	public DataReader<Integer> mrc;

	@DataSeries(key = EncodingKey.NP_NextFragmentAlignmentStart, type = DataSeriesType.INT)
	public DataReader<Integer> malsc;

	@DataSeries(key = EncodingKey.TS_InsetSize, type = DataSeriesType.INT)
	public DataReader<Integer> tsc;

	public static int detachedCount = 0;
	private int recordCount = 0;
	private CramRecord prevRecord;

	@DataSeries(key = EncodingKey.TM_TestMark, type = DataSeriesType.INT)
	public DataReader<Integer> testC;

	@DataSeries(key = EncodingKey.TL_TagIdList, type = DataSeriesType.INT)
	public DataReader<Integer> tagIdListCodec;

	@DataSeries(key = EncodingKey.RI_RefId, type = DataSeriesType.INT)
	public DataReader<Integer> refIdCodec;

	@DataSeries(key = EncodingKey.RS_RefSkip, type = DataSeriesType.INT)
	public DataReader<Integer> refSkipCodec;

	public int refId;
	public SubstitutionMatrix substitutionMatrix;
	public boolean AP_delta = true;

	public void read(CramRecord r) throws IOException {
		try {
			// int mark = testC.readData();
			// if (Writer.TEST_MARK != mark) {
			// System.err.println("Record counter=" + recordCount);
			// System.err.println(r.toString());
			// throw new RuntimeException("Test mark not found.");
			// }

			r.setFlags(bitFlagsC.readData());
			r.setCompressionFlags(compBitFlagsC.readData());
			if (refId == -2)
				r.sequenceId = refIdCodec.readData();
			else
				r.sequenceId = refId;

			r.setReadLength(readLengthC.readData());
			if (AP_delta)
				r.alignmentStartOffsetFromPreviousRecord = alStartC.readData();
			else
				r.setAlignmentStart(alStartC.readData());
			r.setReadGroupID(readGroupC.readData());

			if (captureReadNames) {
				r.setReadName(new String(readNameC.readData(), charset));
			}

			// mate record:
			if (r.isDetached()) {
				r.setMateFlags(mbfc.readData());
				if (!captureReadNames)
					r.setReadName(new String(readNameC.readData(), charset));

				r.mateSequnceID = mrc.readData();
				r.mateAlignmentStart = malsc.readData();
				r.templateSize = tsc.readData();
				detachedCount++;
			} else if (r.isHasMateDownStream())
				r.setRecordsToNextFragment(distanceC.readData());

			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				int tagCount = ids.length;
				r.tags = new ReadTag[tagCount];
				for (int i = 0; i < ids.length; i++) {
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					byte[] data = null;
					try {
						data = dataReader.readData();
					} catch (EOFException e) {
						throw e;
					}
					ReadTag tag = new ReadTag(id, data);
					r.tags[i] = tag;
				}
			}

			if (!r.isSegmentUnmapped()) {
				// writing read features:
				int size = nfc.readData();
				int prevPos = 0;
				java.util.List<ReadFeature> rf = new LinkedList<ReadFeature>();
				r.setReadFeatures(rf);
				for (int i = 0; i < size; i++) {
					Byte operator = fc.readData();

					int pos = prevPos + fp.readData();
					prevPos = pos;

					switch (operator) {
					case ReadBase.operator:
						ReadBase rb = new ReadBase(pos, bc.readData(),
								qc.readData());
						rf.add(rb);
						break;
					case Substitution.operator:
						Substitution sv = new Substitution();
						sv.setPosition(pos);
						byte code = bsc.readData();
						sv.setCode(code);
						// sv.setBaseChange(new BaseChange(bsc.readData()));
						rf.add(sv);
						break;
					case Insertion.operator:
						Insertion iv = new Insertion(pos, inc.readData());
						rf.add(iv);
						break;
					case SoftClip.operator:
						SoftClip fv = new SoftClip(pos,
								softClipCodec.readData());
						rf.add(fv);
						break;
					case Deletion.operator:
						Deletion dv = new Deletion(pos, dlc.readData());
						rf.add(dv);
						break;
					case RefSkip.operator:
						RefSkip rsv = new RefSkip(pos, refSkipCodec.readData());
						rf.add(rsv);
						break;
					case InsertBase.operator:
						InsertBase ib = new InsertBase(pos, bc.readData());
						rf.add(ib);
						break;
					case BaseQualityScore.operator:
						BaseQualityScore bqs = new BaseQualityScore(pos,
								qc.readData());
						rf.add(bqs);
						break;
					default:
						throw new RuntimeException(
								"Unknown read feature operator: " + operator);
					}
				}

				// mapping quality:
				r.setMappingQuality(mqc.readData());
				if (r.isForcePreserveQualityScores()) {
					// byte[] qs = new byte[r.getReadLength()];
					// for (int i = 0; i < qs.length; i++)
					// qs[i] = qc.readData();
					byte[] qs = qcArray.readDataArray(r.getReadLength());
					r.setQualityScores(qs);
				}
			} else {
				byte[] bases = new byte[r.getReadLength()];
				for (int i = 0; i < bases.length; i++)
					bases[i] = bc.readData();
				r.setReadBases(bases);

				if (r.isForcePreserveQualityScores()) {
					// byte[] qs = new byte[r.getReadLength()];
					// for (int i = 0; i < qs.length; i++)
					// qs[i] = qc.readData();
					byte[] qs = qcArray.readDataArray(r.getReadLength());
					r.setQualityScores(qs);
				}
			}

			recordCount++;

			prevRecord = r;
		} catch (Exception e) {
			if (prevRecord != null)
				System.err
						.printf("Failed at record %d. Here is the previously read record: %s\n",
								recordCount, prevRecord.toString());
			throw new RuntimeException(e);
		}
	}
}
