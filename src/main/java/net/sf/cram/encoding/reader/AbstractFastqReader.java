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

import java.io.EOFException;
import java.io.IOException;

import net.sf.cram.common.Utils;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.ReadTag;

public abstract class AbstractFastqReader extends AbstractReader {
	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();
	public boolean reverseNegativeReads = true;
	public boolean appendSegmentIndexToReadNames = true;

	public byte[] referenceSequence;
	public int flags;
	public int compressionFlags;
	public int mateFlags;
	public int readLength;
	public int prevAlStart;
	public byte[] readName;

	public byte[] bases = new byte[1024];
	public byte[] scores = new byte[1024];

	/**
	 * For now this is to identify the right buffer to use.
	 * 
	 * @param flags
	 *            read bit flags
	 * @return 0 for non-paired or other rubbish which could not be reliably
	 *         paired, 1 for first in pair and 2 for second in pair
	 */
	protected int getSegmentIndexInTemplate(int flags) {
		if ((flags & CramRecord.MULTIFRAGMENT_FLAG) == 0)
			return 0;

		if ((flags & CramRecord.FIRST_SEGMENT_FLAG) != 0)
			return 1;
		else
			return 2;
	}

	protected abstract byte[] refSeqChanged(int seqID);

	public void read() throws IOException {

		int seqId = refId;
		try {
			flags = bitFlagsC.readData();

			compressionFlags = compBitFlagsC.readData();
			if (refId == -2) {
				seqId = refIdCodec.readData();
			}

			readLength = readLengthC.readData();
			if (AP_delta)
				prevAlStart += alStartC.readData();
			else
				prevAlStart = alStartC.readData();

			readGroupC.skip();

			if (captureReadNames)
				readName = readNameC.readData();

			// mate record:
			if ((compressionFlags & CramRecord.DETACHED_FLAG) != 0) {
				mateFlags = mbfc.readData();
				if (!captureReadNames)
					readName = readNameC.readData();

				mrc.skip();
				malsc.skip();
				tsc.skip();
				detachedCount++;
			} else if ((compressionFlags & CramRecord.HAS_MATE_DOWNSTREAM_FLAG) != 0)
				distanceC.readData();

			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				for (int i = 0; i < ids.length; i++) {
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					try {
						dataReader.skip();
					} catch (EOFException e) {
						throw e;
					}
				}
			}

			if ((flags & CramRecord.SEGMENT_UNMAPPED_FLAG) == 0) {
				byte[] refBases = referenceSequence;
				if (seqId != refId)
					refBases = refSeqChanged(seqId);
				rfBuf.readReadFeatures(this);
				rfBuf.restoreReadBases(readLength, prevAlStart, refBases,
						substitutionMatrix, bases);

				mqc.skip();
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			} else {
				bc.readByteArrayInto(bases, 0, readLength);

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			}

			if (reverseNegativeReads
					&& (flags & CramRecord.NEGATIVE_STRAND_FLAG) != 0) {
				Utils.reverseComplement(bases, 0, readLength);
				Utils.reverse(scores, 0, readLength);
			}

			writeRead(readName, flags, bases, scores);

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d. \n", recordCounter);
			if (readName != null)
				System.err.println("read name: " + new String(readName));
			throw new RuntimeException(e);
		}
	}

	protected abstract void writeRead(byte[] name, int flags, byte[] bases,
			byte[] scores);

	public abstract void finish();
}
