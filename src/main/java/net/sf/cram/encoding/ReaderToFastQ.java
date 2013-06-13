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
package net.sf.cram.encoding;

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
				readReadFeatures();
				bases = restoreReadBases();

				mqc.skip();
				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			} else {
				bc.readByteArrayInto(bases, 0, readLength);

				if ((compressionFlags & CramRecord.FORCE_PRESERVE_QS_FLAG) != 0)
					qcArray.readByteArrayInto(scores, 0, readLength);
			}

			correctBases();

			buf.put((byte) '@');
			buf.put(readName);
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
				readFeatureBuffer.putInt(hardClipCodec.readData());
				break;
			case Padding.operator:
				readFeatureBuffer.putInt(paddingCodec.readData());
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
				Arrays.fill(bases, (byte) 'N');
				System.arraycopy(ref, alignmentStart, bases, 0,
						Math.min(readLength, ref.length - alignmentStart));
			} else
				System.arraycopy(ref, alignmentStart, bases, 0, readLength);
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
				posInSeq += readFeatureBuffer.getInt();
				break;
			case RefSkip.operator:
				int len = readFeatureBuffer.getInt();
				posInSeq += len ;
				posInSeq += len ;
				break;
			case Padding.operator:
				posInSeq += readFeatureBuffer.getInt();
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
}
