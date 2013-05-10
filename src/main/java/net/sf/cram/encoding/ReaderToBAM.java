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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.cram.build.CramIO;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.HardClip;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.Padding;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.RefSkip;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.io.ByteBufferUtils;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.ReadTag;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SubstitutionMatrix;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.TextCigarCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;

public class ReaderToBAM extends AbstractReader {
	public static int detachedCount = 0;
	private int recordCounter = 0;
	private CramRecord prevRecord;

	public int refId;
	public SubstitutionMatrix substitutionMatrix;
	public boolean AP_delta = true;

	public byte[] buf = new byte[1024 * 1024 * 100];
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
	private byte[] bases = new byte[1024*1024], scores = new byte[1024*1024];

	private int[] names = new int[4 * 100000];
	
	private static int counter = 0 ;

	public void read() throws IOException {
		counter++ ;

		// System.out.println(Arrays.toString(Arrays.copyOfRange(buf, 0, 247)));
		// System.out.println(Arrays.toString(Arrays.copyOfRange(buf, 247,
		// 514)));
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
			} else if ((compressionFlags & CramRecord.HAS_MATE_DOWNSTREAM_FLAG) != 0) {
				distances[recordCounter] = distanceC.readData();
				names[recordCounter + distances[recordCounter]] = recordCounter;
			}

			if (!view.isReadNameSet()) {
				if (names[recordCounter] == 0)
					view.setReadName(String.valueOf(recordCounter).getBytes());
				else
					view.setReadName(String.valueOf(names[recordCounter])
							.getBytes());
			}

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

					tagData[tagDataLen++] = (byte) ((id >> 16) & 0xFF);
					tagData[tagDataLen++] = (byte) ((id >> 8) & 0xFF);
					tagData[tagDataLen++] = (byte) (id & 0xFF);
					// System.out.println(ReadTag.intToNameType3Bytes(id) + ": "
					// + Arrays.toString(data)) ;
					System.arraycopy(data, 0, tagData, tagDataLen, data.length);
					tagDataLen += data.length;
				}
				// System.out.println(new String(tagData, 0, tagDataLen));
			}

			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) == 0) {
				readReadFeatures();
				restoreReadBases();

				// mapping quality:
				view.setMappingScore(mqc.readData());
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			} else {
				for (int i = 0; i < readLength; i++)
					bases[i] = bc.readData();

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			}

			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) != 0) {
				Cigar noCigar = TextCigarCodec.getSingleton().decode(
						SAMRecord.NO_ALIGNMENT_CIGAR);
				view.setCigar(noCigar);
			} else
				view.setCigar(getCigar2());

			view.setBases(bases, 0, readLength);
			view.setQualityScores(scores, 0, readLength);

			tagDataLen = 0;
			view.setTagData(tagData, 0, tagDataLen);

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

		int posInRead = 1;
		int alignmentStart = prevAlStart - 1;

		int posInSeq = 0;
		if (!readFeatureBuffer.hasRemaining()) {
			if (ref.length < alignmentStart + readLength) {
				Arrays.fill(bases, 0, readLength, (byte) 'N');
				System.arraycopy(ref, alignmentStart, bases, 0,
						Math.min(readLength, ref.length - alignmentStart));
			} else
				System.arraycopy(ref, alignmentStart, bases, 0, readLength);
			return bases;
		}

//		if (recordCounter == 3988) {
//			System.out.println();
//		}
//		if (counter == 1363989) {
//			System.out.println("breakpoint");
//		}
		for (int r = 0; r < readFeatureSize; r++) {
//			int bufPos = readFeatureBuffer.position() ;
			byte op = readFeatureBuffer.get();
			int rfPos = readFeatureBuffer.getInt();
			
//			if (recordCounter == 3988) {
//				System.out.printf("RFops: %d (%c) at %d, buf pos before: %d, after: %d\n", op, (char)op, rfPos, bufPos, readFeatureBuffer.position());
//				
//			}

			for (; posInRead < rfPos; posInRead++){
//				if (posInRead > readLength) {
//					System.out.println("gotcha" + counter);
//					System.exit(1);
//				}
				bases[posInRead - 1] = ref[alignmentStart + posInSeq++];
			}

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
			case BaseQualityScore.operator:
				readFeatureBuffer.get();
				break;
			}
		}
		for (; posInRead <= readLength
				&& alignmentStart + posInSeq < ref.length; posInRead++)
			bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

		return bases;
	}

	private final void correctBases() {
		for (int i = 0; i < readLength; i++) {
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

	private final Cigar getCigar2() {
		readFeatureBuffer.rewind();
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
				readFeatureBuffer
						.position(readFeatureBuffer.position() + rfLen);
				break;
			case SoftClip.operator:
				co = CigarOperator.SOFT_CLIP;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer
						.position(readFeatureBuffer.position() + rfLen);
				break;
			case HardClip.operator:
				co = CigarOperator.HARD_CLIP;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer
						.position(readFeatureBuffer.position() + rfLen);
				break;
			case InsertBase.operator:
				co = CigarOperator.INSERTION;
				rfLen = 1;
				readFeatureBuffer.get();
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
				readFeatureBuffer.get();
				break;
			case BaseQualityScore.operator:
				readFeatureBuffer.get();
				continue ;
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

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);

		File cramFile = new File(args[0]);
		File refFile = new File(args[1]);
		File bamFile = new File(cramFile.getAbsolutePath() + ".bam");

		ReferenceSource referenceSource = new ReferenceSource(refFile);

		OutputStream os = (new BufferedOutputStream(new FileOutputStream(
				bamFile)));

		byte[] ref = null;

		InputStream is = new FileInputStream(cramFile);

		CramHeader cramHeader = CramIO.readCramHeader(is);
		// ByteArrayOutputStream bamBAOS = new ByteArrayOutputStream();
		BlockCompressedOutputStream bcos = new BlockCompressedOutputStream(os,
				null);
		bcos.write("BAM\1".getBytes());
		bcos.write(CramIO.toByteArray(cramHeader.samFileHeader));
		ByteBufferUtils.writeInt32(cramHeader.samFileHeader
				.getSequenceDictionary().size(), bcos);
		for (final SAMSequenceRecord sequenceRecord : cramHeader.samFileHeader
				.getSequenceDictionary().getSequences()) {
			byte[] bytes = sequenceRecord.getSequenceName().getBytes();
			ByteBufferUtils.writeInt32(bytes.length + 1, bcos);
			bcos.write(sequenceRecord.getSequenceName().getBytes());
			bcos.write(0);
			ByteBufferUtils
					.writeInt32(sequenceRecord.getSequenceLength(), bcos);
		}

		Container container = null;
		ReaderToBAM reader = new ReaderToBAM();
		DataReaderFactory f = new DataReaderFactory();
		while (true) {
			long containerReadNanos = System.nanoTime() ;
			container = CramIO.readContainer(is);
			if (container == null) break ;
			containerReadNanos = System.nanoTime() -containerReadNanos;
					

			long unknownNanos = 0 ;
			long viewBuildNanos= 0 ;
			long bamWriteNanos = 0 ;
			long delta = 0 ;
			for (Slice s : container.slices) {
				delta = System.nanoTime() ;
				if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					SAMSequenceRecord sequence = cramHeader.samFileHeader
							.getSequence(s.sequenceId);
					ref = referenceSource.getReferenceBases(sequence, true);
				}
				Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (Integer exId : s.external.keySet()) {
					inputMap.put(exId,
							new ByteArrayInputStream(s.external.get(exId)
									.getRawContent()));
				}

				reader.ref = ref;
				reader.prevAlStart = s.alignmentStart;
				reader.substitutionMatrix = container.h.substitutionMatrix;
				reader.recordCounter = 0;
				reader.view = new BAMRecordView(reader.buf);
				f.buildReader(reader, new DefaultBitInputStream(
						new ByteArrayInputStream(s.coreBlock.getRawContent())),
						inputMap, container.h, s.sequenceId);
				
				delta = System.nanoTime() -delta;
				unknownNanos += delta ;

				delta = System.nanoTime() ;
				int len = 0;
				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
					len += reader.view.finish();
				}
				delta = System.nanoTime() -delta;
				viewBuildNanos+=delta ;

				// System.out.println(Arrays.toString(Arrays.copyOfRange(reader.buf,
				// 0, len)));
				delta = System.nanoTime() ;
				bcos.write(reader.buf, 0, len);
				delta = System.nanoTime() -delta;
				bamWriteNanos += delta ;
			}
			System.out.printf("Container read in %dms, views build in %dms, bam written in %dms, unknown %dms.\n", containerReadNanos/1000000, viewBuildNanos/1000000, bamWriteNanos/1000000, unknownNanos/1000000);
		}
		// os.close();
		bcos.close();

		// SAMFileReader samFileReader = new SAMFileReader(
		// new ByteArrayInputStream(bamBAOS.toByteArray()));
		// System.out.println(samFileReader.getFileHeader().getTextHeader());
		//
		// System.out.println();
		//
		// SAMRecordIterator iterator = samFileReader.iterator();
		// while (iterator.hasNext()) {
		// SAMRecord next = iterator.next();
		// System.out.print(next.getSAMString());
		// }
		// samFileReader.close();
	}
}
