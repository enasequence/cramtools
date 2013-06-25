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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.cram.build.CramIO;
import net.sf.cram.common.Utils;
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
import net.sf.samtools.Defaults;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.TextCigarCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;

public class ReaderToBAM extends AbstractReader {
	protected static int detachedCount = 0;
	protected int recordCounter = 0;
	protected CramRecord prevRecord;

	public int refId;
	public SubstitutionMatrix substitutionMatrix;
	public boolean AP_delta = true;

	public byte[] buf = new byte[1024 * 1024 * 100];
	public int[] index = new int[4 * 100000];
	public int[] distances = new int[4 * 100000];
	private int[] names = new int[4 * 100000];
	private int[] next = new int[distances.length];
	private int[] prev = new int[distances.length];

	private BAMRecordView view = new BAMRecordView(buf);

	private int flags;
	private int compressionFlags;
	private int readLength;
	public int prevAlStart = 1;
	private int readGroupID;
	private byte mateFlags;

	private int tagDataLen;
	private byte[] tagData = new byte[1024 * 1024];

	public byte[] ref;
	private byte[] bases = new byte[1024 * 1024],
			scores = new byte[1024 * 1024];

	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();

	private byte[][] readGroups = null;

	public void read() throws IOException {
		index[recordCounter] = view.position();

		try {
			flags = bitFlagsC.readData();

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
				distances[recordCounter] = 0;
				next[recordCounter] = -1;
				prev[recordCounter] = -1;
				flags |= (mateFlags & CramRecord.MATE_NEG_STRAND_FLAG) != 0 ? CramRecord.BAM_FLAGS.MATE_STRAND_FLAG
						: 0;
				flags |= (mateFlags & CramRecord.MATE_UNMAPPED_FLAG) != 0 ? CramRecord.BAM_FLAGS.MATE_UNMAPPED_FLAG
						: 0;
			} else if ((compressionFlags & CramRecord.HAS_MATE_DOWNSTREAM_FLAG) != 0) {
				mateFlags = 0;
				distances[recordCounter] = distanceC.readData();
				next[recordCounter] = recordCounter + distances[recordCounter]
						+ 1;
				prev[next[recordCounter]] = recordCounter;
				names[recordCounter + distances[recordCounter]] = recordCounter;
			}
			view.setFlags(flags);

			if (!view.isReadNameSet()) {
				if (names[recordCounter] == 0)
					view.setReadName(String.valueOf(recordCounter).getBytes());
				else
					view.setReadName(String.valueOf(names[recordCounter])
							.getBytes());
			}

			tagDataLen = 0;
			if (readGroupID >= 0) {
				tagData[tagDataLen++] = (byte) 'R';
				tagData[tagDataLen++] = (byte) 'G';
				tagData[tagDataLen++] = (byte) 'Z';

				System.arraycopy(readGroups[readGroupID], 0, tagData,
						tagDataLen, readGroups[readGroupID].length);
				tagDataLen += readGroups[readGroupID].length;

				tagData[tagDataLen++] = (byte) 0;
			}
			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				for (int i = 0; i < ids.length; i++) {
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					byte[] data = dataReader.readData();

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
				view.setMappingScore(SAMRecord.NO_MAPPING_QUALITY);
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

			view.setTagData(tagData, 0, tagDataLen);

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d, read len=%d\n.",
					recordCounter, readLength);
			throw new RuntimeException(e);
		}
	}

	private int getFlagsForRecord(int record) {
		view.start = index[record];
		return view.getFlags();
	}

	private void setFlagsForRecord(int record, int flags) {
		view.start = index[record];
		view.setFlags(flags);
	}

	private int getAlignmentStartForRecord(int record) {
		view.start = index[record];
		return view.getAlignmentStart();
	}

	/**
	 * @param record
	 * @return inclusive 0-based coordinate of the last base alignment
	 */
	private int getAlignmentEndForRecord(int record) {
		view.start = index[record];
		return view.calculateAlignmentEnd();
	}

	private int getRefIDForRecord(int record) {
		view.start = index[record];
		return view.getRefID();
	}

	private void setInsertSizeForRecord(int record, int insertSize) {
		view.start = index[record];
		view.setInsertSize(insertSize);
	}

	private void setMateRefIDForRecord(int record, int refId) {
		view.start = index[record];
		view.setMateRefID(refId);
	}

	private void setMateAlignmentStartForRecord(int record, int astart) {
		view.start = index[record];
		view.setMateAlStart(astart);
	}

	private boolean recordHasMoreMates(int record) {
		return next[record] > 0;
	}

	private int nextRecord(int record) {
		if (next[record] > 0)
			return next[record];

		return -1;
	}

	private boolean isFirstInPairBitOnForRecord(int record) {
		return (getFlagsForRecord(record) & CramRecord.FIRST_SEGMENT_FLAG) != 0;
	}

	public void fixMateInfo() {
		int fixes = 0;
		for (int record = 0; record < recordCounter; record++) {
			if (prev[record] >= 0)
				// skip non-first reads
				continue;

			if (recordHasMoreMates(record)) {
				int id1 = record;
				int id2 = record;
				int refid = getRefIDForRecord(record);
				do {
					id1 = id2;
					id2 = nextRecord(id1);

					int flags1 = getFlagsForRecord(id1);
					int flags2 = getFlagsForRecord(id2);

					if (refid != getRefIDForRecord(id2))
						refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;

					if ((flags2 & CramRecord.BAM_FLAGS.READ_UNMAPPED_FLAG) != 0) {
						flags1 |= CramRecord.BAM_FLAGS.MATE_UNMAPPED_FLAG;
						refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
					}
					if ((flags1 & CramRecord.BAM_FLAGS.READ_UNMAPPED_FLAG) != 0) {
						flags2 |= CramRecord.BAM_FLAGS.MATE_UNMAPPED_FLAG;
						refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
					}

					if ((flags2 & CramRecord.BAM_FLAGS.READ_STRAND_FLAG) != 0)
						flags1 |= CramRecord.BAM_FLAGS.MATE_STRAND_FLAG;
					if ((flags1 & CramRecord.BAM_FLAGS.READ_STRAND_FLAG) != 0)
						flags2 |= CramRecord.BAM_FLAGS.MATE_STRAND_FLAG;

					setFlagsForRecord(id1, flags1);
					setFlagsForRecord(id2, flags2);

					setMateRefIDForRecord(id1, getRefIDForRecord(id2));
					setMateAlignmentStartForRecord(id1,
							1 + getAlignmentStartForRecord(id2));
					fixes++;
				} while (recordHasMoreMates(id2));

				id1 = record;
				int flags1 = getFlagsForRecord(id1);
				int flags2 = getFlagsForRecord(id2);

				if (refid != getRefIDForRecord(id1))
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;

				if ((flags2 & CramRecord.BAM_FLAGS.READ_UNMAPPED_FLAG) != 0) {
					flags1 |= CramRecord.BAM_FLAGS.MATE_UNMAPPED_FLAG;
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
				}
				if ((flags1 & CramRecord.BAM_FLAGS.READ_UNMAPPED_FLAG) != 0) {
					flags2 |= CramRecord.BAM_FLAGS.MATE_UNMAPPED_FLAG;
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
				}

				if ((flags2 & CramRecord.BAM_FLAGS.READ_STRAND_FLAG) != 0)
					flags1 |= CramRecord.BAM_FLAGS.MATE_STRAND_FLAG;
				if ((flags1 & CramRecord.BAM_FLAGS.READ_STRAND_FLAG) != 0)
					flags2 |= CramRecord.BAM_FLAGS.MATE_STRAND_FLAG;

				setFlagsForRecord(id1, flags1);
				setFlagsForRecord(id2, flags2);

				setMateRefIDForRecord(id2, getRefIDForRecord(record));
				setMateAlignmentStartForRecord(id2,
						1 + getAlignmentStartForRecord(record));
				fixes++;

				if (refid == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					id1 = record;
					id2 = record;
					do {
						id1 = id2;
						id2 = nextRecord(id1);

						setInsertSizeForRecord(id1, 0);
						setInsertSizeForRecord(id2, 0);

						fixes++;
					} while (recordHasMoreMates(id2));
				} else
					calculateTemplateSize(record);
			}
		}
	}

	private int calculateTemplateSize(int record) {
		int aleft = getAlignmentStartForRecord(record);
		int aright = getAlignmentEndForRecord(record);

		int id1 = record, id2 = record;
		int tlen = 0;
		int ref = getRefIDForRecord(id1);
		// System.out.println("Calculating template size for record " + record);

		while (recordHasMoreMates(id2)) {
			id2 = nextRecord(id2);
			// System.out.println("\tnext=" + id2);
			if (aleft > getAlignmentStartForRecord(id2))
				aleft = getAlignmentStartForRecord(id2);

			if (aright < getAlignmentEndForRecord(id2))
				aright = getAlignmentEndForRecord(id2);

			if (ref != getRefIDForRecord(id2))
				ref = -1;
		}

		if (ref == -1) {
			tlen = 0;
			id1 = id2 = record;
			setInsertSizeForRecord(id1, tlen);

			while (recordHasMoreMates(id1)) {
				id1 = nextRecord(id1);
				setInsertSizeForRecord(id1, tlen);
			}
		} else {
			tlen = aright - aleft + 1;
			id1 = id2 = record;

			if (getAlignmentStartForRecord(id2) == aleft) {
				if (getAlignmentEndForRecord(id2) != aright)
					setInsertSizeForRecord(id2, tlen);
				else if (isFirstInPairBitOnForRecord(id2))
					setInsertSizeForRecord(id2, tlen);
				else
					setInsertSizeForRecord(id2, -tlen);
			} else
				setInsertSizeForRecord(id2, -tlen);

			while (recordHasMoreMates(id2)) {
				id2 = nextRecord(id2);
				if (getAlignmentStartForRecord(id2) == aleft) {
					if (getAlignmentEndForRecord(id2) != aright)
						setInsertSizeForRecord(id2, tlen);
					else if (isFirstInPairBitOnForRecord(id2))
						setInsertSizeForRecord(id2, tlen);
					else
						setInsertSizeForRecord(id2, -tlen);
				} else
					setInsertSizeForRecord(id2, -tlen);
			}
		}

		return tlen;
	}

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);

		File cramFile = new File(args[0]);
		File refFile = new File(args[1]);
		File bamFile = args.length > 2 ? new File(args[2]) : new File(
				cramFile.getAbsolutePath() + ".bam");

		System.out.println("Compression level=" + Defaults.COMPRESSION_LEVEL);

		ReferenceSource referenceSource = new ReferenceSource(refFile);

		OutputStream os = new BufferedOutputStream(
				new FileOutputStream(bamFile));

		byte[] ref = null;

		InputStream is = new FileInputStream(cramFile);

		CramHeader cramHeader = CramIO.readCramHeader(is);
		OutputStream bcos = new BlockCompressedOutputStream(os, null);
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

		List<SAMReadGroupRecord> groups = cramHeader.samFileHeader
				.getReadGroups();
		byte[][] rg = new byte[groups.size()][];
		for (int i = 0; i < groups.size(); i++) {
			rg[i] = groups.get(i).getId().getBytes();
		}

		int seqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
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
				if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX
						&& s.sequenceId != seqId) {
					SAMSequenceRecord sequence = cramHeader.samFileHeader
							.getSequence(s.sequenceId);
					ref = referenceSource.getReferenceBases(sequence, true);
					Utils.upperCase(ref);
					seqId = s.sequenceId;
				}
				Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (Integer exId : s.external.keySet()) {
					inputMap.put(exId,
							new ByteArrayInputStream(s.external.get(exId)
									.getRawContent()));
				}

				reader.readGroups = rg;
				reader.ref = ref;
				reader.refId = s.sequenceId;
				reader.prevAlStart = s.alignmentStart;
				reader.substitutionMatrix = container.h.substitutionMatrix;
				reader.recordCounter = 0;
				reader.view = new BAMRecordView(reader.buf);
				f.buildReader(reader, new DefaultBitInputStream(
						new ByteArrayInputStream(s.coreBlock.getRawContent())),
						inputMap, container.h, s.sequenceId);
				Arrays.fill(reader.prev, -1);
				Arrays.fill(reader.next, -1);

				delta = System.nanoTime() - delta;
				unknownNanos += delta;

				delta = System.nanoTime();
				int len = 0;
				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
					len += reader.view.finish();
				}
				reader.fixMateInfo();
				delta = System.nanoTime() - delta;
				viewBuildNanos += delta;

				delta = System.nanoTime();
				bcos.write(reader.buf, 0, len);
				delta = System.nanoTime() - delta;
				bamWriteNanos += delta;
			}
			System.err
					.printf("Container read in %dms, views build in %dms, bam written in %dms, unknown %dms.\n",
							containerReadNanos / 1000000,
							viewBuildNanos / 1000000, bamWriteNanos / 1000000,
							unknownNanos / 1000000);
		}
		bcos.close();
	}
}
