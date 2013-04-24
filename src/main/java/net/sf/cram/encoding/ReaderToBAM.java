package net.sf.cram.encoding;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.cram.encoding.read_features.BaseChange;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.HardClip;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.Padding;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.RefSkip;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.EncodingKey;
import net.sf.cram.structure.ReadTag;
import net.sf.cram.structure.SubstitutionMatrix;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.TextCigarCodec;

public class ReaderToBAM {
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
	
	@DataSeries(key = EncodingKey.HC_HardClip, type = DataSeriesType.BYTE_ARRAY)
	public DataReader<byte[]> hardClipCodec;

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
	private int recordCounter = 0;
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

	public byte[] buf = new byte[1024 * 1024 * 10];
	public int[] index = new int[4 * 100000];
	public int[] distances = new int[4 * 100000];
	private ByteBuffer readFeatureBuffer = ByteBuffer.allocate(1024);
	private BAMRecordView view = new BAMRecordView(buf);

	private int flags;
	private int compressionFlags;
	private int readLength;
	public int prevAlStart = 1;
	private int readGroupID;
	private int mateFlags;

	private int tagDataLen;
	private byte[] tagData = new byte[1024 * 1024];

	public byte[] ref;
	private int readFeatureSize;

	public void read(CramRecord r) throws IOException {

		try {
			flags = bitFlagsC.readData();
			view.setFlags(flags);

			compressionFlags = compBitFlagsC.readData();
			if (refId == -2)
				view.setRefID(refIdCodec.readData());
			else
				view.setRefID(refId);

			readLength = readLengthC.readData();
			if (AP_delta)
				prevAlStart += alStartC.readData();
			else
				prevAlStart = alStartC.readData();

			view.setAlignmentStart(prevAlStart);

			readGroupID = readGroupC.readData();

			if (captureReadNames)
				view.setReadName(readNameC.readData());

			// mate record:
			if ((compressionFlags & CramRecord.DETACHED_FLAG) != 0) {
				mateFlags = mbfc.readData();
				if (!captureReadNames)
					view.setReadName(readNameC.readData());

				view.setMateRefID(mrc.readData());
				view.setMateAlStart(malsc.readData());
				view.setInsertSize(tsc.readData());
				detachedCount++;
			} else if ((compressionFlags & CramRecord.HAS_MATE_DOWNSTREAM_FLAG) != 0)
				distances[recordCounter] = distanceC.readData();

			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				for (int i = 0; i < ids.length; i++) {
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					byte[] data = null;
					try {
						data = dataReader.readData();
					} catch (EOFException e) {
						throw e;
					}

					tagData[tagDataLen++] = (byte) (id & 0xFF);
					tagData[tagDataLen++] = (byte) ((id >> 8) & 0xFF);
					tagData[tagDataLen++] = (byte) ((id >> 16) & 0xFF);
					System.arraycopy(data, 0, tagData, tagDataLen, data.length);
					tagDataLen += data.length;
				}
			}

			byte[] bases;
			byte[] scores = null;
			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) == 0) {
				readReadFeatures();
				bases = restoreReadBases();

				// mapping quality:
				view.setMappingScore(mqc.readData());
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					scores = qcArray.readDataArray(readLength);
			} else {
				bases = new byte[readLength];
				for (int i = 0; i < bases.length; i++)
					bases[i] = bc.readData();

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					scores = qcArray.readDataArray(readLength);
			}

			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) != 0) {
				Cigar noCigar = TextCigarCodec.getSingleton().decode(
						SAMRecord.NO_ALIGNMENT_CIGAR);
				view.setCigar(noCigar);
			} else 
				view.setCigar(getCigar2());

			view.setBases(bases);
			
			if (scores != null)
				view.setQualityScores(scores);
			else
				view.setQualityScores(new byte[0]);
			
			view.setTagData(tagData, 0, tagDataLen) ;

			recordCounter++;
		} catch (Exception e) {
			if (prevRecord != null)
				System.err
						.printf("Failed at record %d. Here is the previously read record: %s\n",
								recordCounter, prevRecord.toString());
			throw new RuntimeException(e);
		}
	}

	private final void readReadFeatures() throws IOException {
		readFeatureBuffer.clear();
		readFeatureSize = nfc.readData();
		int prevPos = 0;
		for (int i = 0; i < readFeatureSize; i++) {
			Byte operator = fc.readData();

			int pos = prevPos + fp.readData();
			prevPos = pos;

			readFeatureBuffer.putInt(pos);
			readFeatureBuffer.put(operator);

			switch (operator) {
			case ReadBase.operator:
				readFeatureBuffer.put(bc.readData());
				readFeatureBuffer.put(qc.readData());
				break;
			case Substitution.operator:
				readFeatureBuffer.put(bsc.readData());
				break;
			case Insertion.operator:
				byte[] ins = inc.readData();
				readFeatureBuffer.putInt(ins.length);
				readFeatureBuffer.put(ins);
				break;
			case SoftClip.operator:
				byte[] softClip = softClipCodec.readData();
				readFeatureBuffer.putInt(softClip.length);
				readFeatureBuffer.put(softClip);
				break;
			case Deletion.operator:
				readFeatureBuffer.putInt(dlc.readData());
				break;
			case RefSkip.operator:
				readFeatureBuffer.putInt(refSkipCodec.readData());
				break;
			case InsertBase.operator:
				readFeatureBuffer.put(bc.readData());
				break;
			case BaseQualityScore.operator:
				readFeatureBuffer.put(qc.readData());
				break;
			case HardClip.operator:
				byte[] hardClip = hardClipCodec.readData();
				readFeatureBuffer.putInt(hardClip.length);
				readFeatureBuffer.put(hardClip);
				break;
			default:
				throw new RuntimeException("Unknown read feature operator: "
						+ operator);
			}
		}
		readFeatureBuffer.flip();
	}

	private final byte[] restoreReadBases() {
		readFeatureBuffer.rewind() ;
		byte[] bases = new byte[readLength];

		int posInRead = 1;
		int alignmentStart = prevAlStart - 1;

		int posInSeq = 0;
		if (!readFeatureBuffer.hasRemaining()) {
			if (ref.length < alignmentStart + bases.length) {
				Arrays.fill(bases, (byte) 'N');
				System.arraycopy(ref, alignmentStart, bases, 0,
						Math.min(bases.length, ref.length - alignmentStart));
			} else
				System.arraycopy(ref, alignmentStart, bases, 0, bases.length);
			return bases;
		}

		for (int r = 0; r < readFeatureSize; r++) {
			byte op = readFeatureBuffer.get();
			int rfPos = readFeatureBuffer.getInt();

			for (; posInRead < rfPos; posInRead++)
				bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

			switch (op) {
			case Substitution.operator:
				byte refBase = ref[alignmentStart + posInSeq];
				byte base = substitutionMatrix.base(refBase,
						readFeatureBuffer.get());
				bases[posInRead++ - 1] = base;
				posInSeq++;
				break;
			case Insertion.operator:
				readFeatureBuffer.get(bases, posInRead++ - 1,
						readFeatureBuffer.getInt());
				break;
			case SoftClip.operator:
				readFeatureBuffer.get(bases, posInRead++ - 1,
						readFeatureBuffer.getInt());
				break;
			case HardClip.operator:
				readFeatureBuffer.get(bases, posInRead++ - 1,
						readFeatureBuffer.getInt());
				break;
			case Deletion.operator:
				posInSeq += readFeatureBuffer.getInt();
				break;
			case InsertBase.operator:
				bases[posInRead++ - 1] = readFeatureBuffer.get();
				break;
			case ReadBase.operator:
				bases[posInRead++ - 1] = readFeatureBuffer.get();
				break;
			}
		}
		for (; posInRead <= readLength; posInRead++)
			bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

		for (int i = 0; i < bases.length; i++) {
			switch (bases[i]) {
			case 'a':
				bases[i] = 'A';
				break;
			case 'c':
				bases[i] = 'C';
				break;
			case 'g':
				bases[i] = 'G';
				break;
			case 't':
				bases[i] = 'T';
				break;
			case 'n':
				bases[i] = 'N';
				break;

			default:
				break;
			}
		}

		return bases;
	}

	private final Cigar getCigar2() {
		readFeatureBuffer.rewind() ;
		if (!readFeatureBuffer.hasRemaining()) {
			CigarElement ce = new CigarElement(readLength, CigarOperator.M);
			return new Cigar(Arrays.asList(ce));
		}

		List<CigarElement> list = new ArrayList<CigarElement>();
		int totalOpLen = 1;
		CigarElement ce;
		CigarOperator lastOperator = CigarOperator.MATCH_OR_MISMATCH;
		int lastOpLen = 0;
		int lastOpPos = 1;
		CigarOperator co = null;
		int rfLen = 0;
		for (int r = 0; r < readFeatureSize; r++) {
			byte op = readFeatureBuffer.get();
			int rfPos = readFeatureBuffer.getInt();

			int gap = rfPos - (lastOpPos + lastOpLen);
			if (gap > 0) {
				if (lastOperator != CigarOperator.MATCH_OR_MISMATCH) {
					list.add(new CigarElement(lastOpLen, lastOperator));
					lastOpPos += lastOpLen;
					totalOpLen += lastOpLen;
					lastOpLen = gap;
				} else {
					lastOpLen += gap;
				}

				lastOperator = CigarOperator.MATCH_OR_MISMATCH;
			}

			switch (op) {
			case Insertion.operator:
				co = CigarOperator.INSERTION;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer.position(readFeatureBuffer.position()+rfLen) ;
				break;
			case SoftClip.operator:
				co = CigarOperator.SOFT_CLIP;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer.position(readFeatureBuffer.position()+rfLen) ;
				break;
			case HardClip.operator:
				co = CigarOperator.HARD_CLIP;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer.position(readFeatureBuffer.position()+rfLen) ;
				break;
			case InsertBase.operator:
				co = CigarOperator.INSERTION;
				rfLen = 1;
				break;
			case Deletion.operator:
				co = CigarOperator.DELETION;
				rfLen = readFeatureBuffer.getInt();
				break;
			case RefSkip.operator:
				co = CigarOperator.SKIPPED_REGION;
				rfLen = readFeatureBuffer.getInt();
				break;
			case Padding.operator:
				co = CigarOperator.PADDING;
				rfLen = readFeatureBuffer.getInt();
				break;
			case Substitution.operator:
			case ReadBase.operator:
				co = CigarOperator.MATCH_OR_MISMATCH;
				rfLen = 1;
				break;
			default:
				continue;
			}

			if (lastOperator != co) {
				// add last feature
				if (lastOpLen > 0) {
					list.add(new CigarElement(lastOpLen, lastOperator));
					totalOpLen += lastOpLen;
				}
				lastOperator = co;
				lastOpLen = rfLen;
				lastOpPos = rfPos;
			} else
				lastOpLen += rfLen;

			if (!co.consumesReadBases())
				lastOpPos -= rfLen;
		}

		if (lastOperator != null) {
			if (lastOperator != CigarOperator.M) {
				list.add(new CigarElement(lastOpLen, lastOperator));
				if (readLength >= lastOpPos + lastOpLen) {
					ce = new CigarElement(readLength - (lastOpLen + lastOpPos)
							+ 1, CigarOperator.M);
					list.add(ce);
				}
			} else if (readLength > lastOpPos - 1) {
				ce = new CigarElement(readLength - lastOpPos + 1,
						CigarOperator.M);
				list.add(ce);
			}
		}

		if (list.isEmpty()) {
			ce = new CigarElement(readLength, CigarOperator.M);
			return new Cigar(Arrays.asList(ce));
		}

		return new Cigar(list);
	}
}
