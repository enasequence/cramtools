package net.sf.cram.encoding;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.build.ContainerParser;
import net.sf.cram.build.CramIO;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.HardClip;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.RefSkip;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.ReadTag;
import net.sf.cram.structure.Slice;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMUtils;

public class ReaderToFastQ extends AbstractReader {

	ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 10);
	public int[] index = new int[4 * 100000];
	public int[] distances = new int[4 * 100000];
	private ByteBuffer readFeatureBuffer = ByteBuffer.allocate(1024);

	private int flags;
	private int compressionFlags;
	private int mateFlags;
	private int readLength;
	public int prevAlStart;
	private byte[] readName;

	public byte[] ref;
	private int readFeatureSize;

	public void read() throws IOException {

		try {
			flags = bitFlagsC.readData();

			compressionFlags = compBitFlagsC.readData();
			if (refId == -2)
				refIdCodec.readData();

			readLength = readLengthC.readData();
			if (AP_delta)
				prevAlStart += alStartC.readData();
			else
				prevAlStart = alStartC.readData();

			readGroupC.readData();

			if (captureReadNames)
				readName = readNameC.readData();

			// mate record:
			if ((compressionFlags & CramRecord.DETACHED_FLAG) != 0) {
				mateFlags = mbfc.readData();
				if (!captureReadNames)
					readName = readNameC.readData();

				mrc.readData();
				malsc.readData();
				tsc.readData();
				detachedCount++;
			} else if ((compressionFlags & CramRecord.HAS_MATE_DOWNSTREAM_FLAG) != 0)
				distances[recordCounter] = distanceC.readData();

			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				for (int i = 0; i < ids.length; i++) {
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					try {
						dataReader.readData();
					} catch (EOFException e) {
						throw e;
					}
				}
			}

			byte[] bases;
			byte[] scores = null;
			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) == 0) {
				readReadFeatures();
				bases = restoreReadBases();

				// mapping quality:
				mqc.readData();
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					scores = qcArray.readDataArray(readLength);
			} else {
				bases = new byte[readLength];
				for (int i = 0; i < bases.length; i++)
					bases[i] = bc.readData();

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					scores = qcArray.readDataArray(readLength);
			}
			
			correct(bases) ;
			
			buf.put((byte) '@');
			buf.put(readName);
			buf.put((byte) '\n');
			buf.put(bases);
			buf.put((byte) '\n');
			buf.put((byte) '+');
			buf.put((byte) '\n');
			if (scores != null) {
				for (int i = 0; i < readLength; i++)
					scores[i] += 33;
				buf.put(scores);
			}
			buf.put((byte) '\n');

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d. \n", recordCounter);
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

			readFeatureBuffer.put(operator);
			readFeatureBuffer.putInt(pos);

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
		readFeatureBuffer.rewind();
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
				bases[posInRead - 1] = base;
				posInRead++;
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

		return bases;
	}
	
	private final void correct (byte[] bases) {
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
	}

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.ERROR) ;
		
		File cramFile = new File(args[0]);
		File refFile = new File(args[1]);
		File fqgzFile = new File(cramFile.getAbsolutePath() + ".fq") ;
		
		OutputStream os = (new BufferedOutputStream(new FileOutputStream(fqgzFile))) ;

		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(refFile);
		byte[] ref = null ;

		InputStream is = new FileInputStream(cramFile);

		CramHeader cramHeader = CramIO.readCramHeader(is);
		Container container = null;
		while ((container = CramIO.readContainer(is)) != null) {
			DataReaderFactory f = new DataReaderFactory();

			for (Slice s : container.slices) {
				String seqName = SAMRecord.NO_ALIGNMENT_REFERENCE_NAME;
				if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					SAMSequenceRecord sequence = cramHeader.samFileHeader
							.getSequence(s.sequenceId);
					seqName = sequence.getSequenceName();
					ref = referenceSequenceFile.getSequence(seqName).getBases();

				} 
				Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (Integer exId : s.external.keySet()) {
					inputMap.put(exId,
							new ByteArrayInputStream(s.external.get(exId)
									.getRawContent()));
				}

				ReaderToFastQ reader = new ReaderToFastQ();
				reader.ref = ref;
				reader.prevAlStart = s.alignmentStart;
				reader.substitutionMatrix = container.h.substitutionMatrix;
				f.buildReader(reader, new DefaultBitInputStream(
						new ByteArrayInputStream(s.coreBlock.getRawContent())),
						inputMap, container.h, s.sequenceId);

				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
				}
				reader.buf.flip();
				long sum = 0 ;
//				for (int i=0; i<reader.buf.limit(); i++)
//					sum += reader.buf.get(i) ;
//				System.out.println(sum);
				os.write(reader.buf.array(), 0, reader.buf.limit());
				reader.buf.clear();
			}
		}
		os.close() ;
	}
}
