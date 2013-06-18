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
import java.nio.ByteBuffer;
import java.util.Arrays;

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
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.ReadTag;

public class ReaderToFastQ extends AbstractReader {

	public ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 10);
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

	byte[] bases = new byte[1024];
	byte[] scores = new byte[1024];

	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();

	public void read() throws IOException {

		try {
			flags = bitFlagsC.readData();

			compressionFlags = compBitFlagsC.readData();
			if (refId == -2)
				refIdCodec.skip();

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
				distances[recordCounter] = distanceC.readData();

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
				rfBuf.readReadFeatures(this);
				rfBuf.restoreReadBases(readLength, prevAlStart, ref,
						substitutionMatrix, bases);

				mqc.skip();
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			} else {
				bc.readByteArrayInto(bases, 0, readLength);

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			}

			buf.put((byte) '@');
			buf.put(readName);
			if ((flags & CramRecord.MULTIFRAGMENT_FLAG) != 0) {
				buf.put((byte) '.');
				buf.put((flags & CramRecord.FIRST_SEGMENT_FLAG) != 0 ? (byte) '1'
						: (byte) '2');
			}
			buf.put((byte) '\n');
			buf.put(bases, 0, readLength);
			buf.put((byte) '\n');
			buf.put((byte) '+');
			buf.put((byte) '\n');
			if (scores != null) {
				for (int i = 0; i < readLength; i++)
					scores[i] += 33;
				buf.put(scores, 0, readLength);
			}
			buf.put((byte) '\n');

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d. \n", recordCounter);
			throw new RuntimeException(e);
		}
	}
}
