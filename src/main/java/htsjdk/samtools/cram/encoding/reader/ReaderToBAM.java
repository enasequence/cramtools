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
package htsjdk.samtools.cram.encoding.reader;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.ReadTag;
import htsjdk.samtools.cram.structure.SubstitutionMatrix;

import java.io.IOException;

public class ReaderToBAM extends AbstractReader {
	protected static int detachedCount = 0;
	protected int recordCounter = 0;
	protected CramCompressionRecord prevRecord;

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
	public static final int maxReadBufferLength = 1024 * 1024;
	public byte[] bases = new byte[maxReadBufferLength];
	public byte[] scores = new byte[maxReadBufferLength];

	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();

	private byte[][] readGroups = null;

	public void read() throws IOException {
		index[recordCounter] = view.position();

		try {
			flags = bitFlagsCodec.readData();

			compressionFlags = compressionBitFlagsCodec.readData();
			if (refId == -2)
				view.setRefID(refIdCodec.readData());
			else
				view.setRefID(refId);

			readLength = readLengthCodec.readData();
			if (AP_delta)
				prevAlStart += alignmentStartCodec.readData();
			else
				prevAlStart = alignmentStartCodec.readData();

			view.setAlignmentStart(prevAlStart);

			readGroupID = readGroupCodec.readData();

			if (captureReadNames)
				view.setReadName(readNameCodec.readData());

			// mate record:
			if ((compressionFlags & CramFlags.DETACHED_FLAG) != 0) {
				mateFlags = mateBitFlagCodec.readData();
				if (!captureReadNames)
					view.setReadName(readNameCodec.readData());

				view.setMateRefID(mateReferenceIdCodec.readData());
				view.setMateAlStart(mateAlignmentStartCodec.readData());
				view.setInsertSize(insertSizeCodec.readData());
				detachedCount++;
				distances[recordCounter] = 0;
				next[recordCounter] = -1;
				prev[recordCounter] = -1;
				flags |= (mateFlags & CramFlags.MATE_NEG_STRAND_FLAG) != 0 ? BamFlags.MATE_STRAND_FLAG : 0;
				flags |= (mateFlags & CramFlags.MATE_UNMAPPED_FLAG) != 0 ? BamFlags.MATE_UNMAPPED_FLAG : 0;
			} else if ((compressionFlags & CramFlags.HAS_MATE_DOWNSTREAM_FLAG) != 0) {
				mateFlags = 0;
				distances[recordCounter] = distanceToNextFragmentCodec.readData();
				next[recordCounter] = recordCounter + distances[recordCounter] + 1;
				prev[next[recordCounter]] = recordCounter;
				names[recordCounter + distances[recordCounter]] = recordCounter;
			}
			view.setFlags(flags);

			if (!view.isReadNameSet()) {
				if (names[recordCounter] == 0)
					view.setReadName(String.valueOf(recordCounter).getBytes());
				else
					view.setReadName(String.valueOf(names[recordCounter]).getBytes());
			}

			tagDataLen = 0;
			if (readGroupID >= 0) {
				tagData[tagDataLen++] = (byte) 'R';
				tagData[tagDataLen++] = (byte) 'G';
				tagData[tagDataLen++] = (byte) 'Z';

				System.arraycopy(readGroups[readGroupID], 0, tagData, tagDataLen, readGroups[readGroupID].length);
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

			if ((flags & CramFlags.SEGMENT_UNMAPPED_FLAG) == 0) {
				rfBuf.readReadFeatures(this);
				rfBuf.restoreReadBases(readLength, prevAlStart, ref, substitutionMatrix, bases);

				// mapping quality:
				view.setMappingScore(mappingScoreCodec.readData());
				if ((compressionFlags & CramFlags.FORCE_PRESERVE_QS_FLAG) != 0)
					for (int i=0; i<readLength; i++)
					scores[i]=qualityScoreCodec.readData();
			} else {
				view.setMappingScore(SAMRecord.NO_MAPPING_QUALITY);
				for (int i = 0; i < readLength; i++)
					bases[i] = baseCodec.readData();

				if ((compressionFlags & CramFlags.FORCE_PRESERVE_QS_FLAG) != 0)
					for (int i=0; i<readLength; i++)
						scores[i]=qualityScoreCodec.readData();
			}

			if ((flags & CramFlags.SEGMENT_UNMAPPED_FLAG) != 0) {
				Cigar noCigar = TextCigarCodec.decode(SAMRecord.NO_ALIGNMENT_CIGAR);
				view.setCigar(noCigar);
			} else
				view.setCigar(rfBuf.getCigar(readLength));

			view.setBases(bases, 0, readLength);
			view.setQualityScores(scores, 0, readLength);

			view.setTagData(tagData, 0, tagDataLen);

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d, read len=%d\n.", recordCounter, readLength);
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
		return (getFlagsForRecord(record) & CramFlags.FIRST_SEGMENT_FLAG) != 0;
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

					if ((flags2 & BamFlags.READ_UNMAPPED_FLAG) != 0) {
						flags1 |= BamFlags.MATE_UNMAPPED_FLAG;
						refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
					}
					if ((flags1 & BamFlags.READ_UNMAPPED_FLAG) != 0) {
						flags2 |= BamFlags.MATE_UNMAPPED_FLAG;
						refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
					}

					if ((flags2 & BamFlags.READ_STRAND_FLAG) != 0)
						flags1 |= BamFlags.MATE_STRAND_FLAG;
					if ((flags1 & BamFlags.READ_STRAND_FLAG) != 0)
						flags2 |= BamFlags.MATE_STRAND_FLAG;

					setFlagsForRecord(id1, flags1);
					setFlagsForRecord(id2, flags2);

					setMateRefIDForRecord(id1, getRefIDForRecord(id2));
					setMateAlignmentStartForRecord(id1, 1 + getAlignmentStartForRecord(id2));
					fixes++;
				} while (recordHasMoreMates(id2));

				id1 = record;
				int flags1 = getFlagsForRecord(id1);
				int flags2 = getFlagsForRecord(id2);

				if (refid != getRefIDForRecord(id1))
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;

				if ((flags2 & BamFlags.READ_UNMAPPED_FLAG) != 0) {
					flags1 |= BamFlags.MATE_UNMAPPED_FLAG;
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
				}
				if ((flags1 & BamFlags.READ_UNMAPPED_FLAG) != 0) {
					flags2 |= BamFlags.MATE_UNMAPPED_FLAG;
					refid = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
				}

				if ((flags2 & BamFlags.READ_STRAND_FLAG) != 0)
					flags1 |= BamFlags.MATE_STRAND_FLAG;
				if ((flags1 & BamFlags.READ_STRAND_FLAG) != 0)
					flags2 |= BamFlags.MATE_STRAND_FLAG;

				setFlagsForRecord(id1, flags1);
				setFlagsForRecord(id2, flags2);

				setMateRefIDForRecord(id2, getRefIDForRecord(record));
				setMateAlignmentStartForRecord(id2, 1 + getAlignmentStartForRecord(record));
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
}
