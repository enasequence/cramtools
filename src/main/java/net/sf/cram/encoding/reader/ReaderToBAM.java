/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram.encoding.reader;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import net.sf.cram.build.CramIO;
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
	private byte[] bases = new byte[1024 * 1024],
			scores = new byte[1024 * 1024];

	private int[] names = new int[4 * 100000];

	private static int counter = 0;

	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();

	public void read() throws IOException {
		counter++;

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
					System.arraycopy(data, 0, tagData, tagDataLen, data.length);
					tagDataLen += data.length;
				}
			}

			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) == 0) {
				rfBuf.readReadFeatures(this);
				rfBuf.restoreReadBases(readLength, prevAlStart, ref,
						substitutionMatrix, bases);

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
				view.setCigar(rfBuf.getCigar(readLength));

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
			long containerReadNanos = System.nanoTime();
			container = CramIO.readContainer(is);
			if (container == null)
				break;
			containerReadNanos = System.nanoTime() - containerReadNanos;

			long unknownNanos = 0;
			long viewBuildNanos = 0;
			long bamWriteNanos = 0;
			long delta = 0;
			for (Slice s : container.slices) {
				delta = System.nanoTime();
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

				delta = System.nanoTime() - delta;
				unknownNanos += delta;

				delta = System.nanoTime();
				int len = 0;
				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
					len += reader.view.finish();
				}
				delta = System.nanoTime() - delta;
				viewBuildNanos += delta;

				delta = System.nanoTime();
				bcos.write(reader.buf, 0, len);
				delta = System.nanoTime() - delta;
				bamWriteNanos += delta;
			}
			System.out
					.printf("Container read in %dms, views build in %dms, bam written in %dms, unknown %dms.\n",
							containerReadNanos / 1000000,
							viewBuildNanos / 1000000, bamWriteNanos / 1000000,
							unknownNanos / 1000000);
		}
		bcos.close();
	}
}
