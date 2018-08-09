/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package htsjdk.samtools.cram.encoding.reader;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.cram.encoding.readfeatures.BaseQualityScore;
import htsjdk.samtools.cram.encoding.readfeatures.Bases;
import htsjdk.samtools.cram.encoding.readfeatures.Deletion;
import htsjdk.samtools.cram.encoding.readfeatures.HardClip;
import htsjdk.samtools.cram.encoding.readfeatures.InsertBase;
import htsjdk.samtools.cram.encoding.readfeatures.Insertion;
import htsjdk.samtools.cram.encoding.readfeatures.Padding;
import htsjdk.samtools.cram.encoding.readfeatures.ReadBase;
import htsjdk.samtools.cram.encoding.readfeatures.RefSkip;
import htsjdk.samtools.cram.encoding.readfeatures.Scores;
import htsjdk.samtools.cram.encoding.readfeatures.SoftClip;
import htsjdk.samtools.cram.encoding.readfeatures.Substitution;
import htsjdk.samtools.cram.structure.SubstitutionMatrix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ReadFeatureBuffer {
	private ByteBuffer readFeatureBuffer = ByteBuffer.allocate(1024 * 1024);
	private int readFeatureSize;

	public final void readReadFeatures(AbstractReader reader) throws IOException {
		readFeatureBuffer.clear();
		readFeatureSize = reader.numberOfReadFeaturesCodec.readData();
		int prevPos = 0;
		for (int i = 0; i < readFeatureSize; i++) {
			Byte operator = reader.readFeatureCodeCodec.readData();
			int pos = prevPos + reader.readFeaturePositionCodec.readData();
			prevPos = pos;

			readFeatureBuffer.put(operator);
			readFeatureBuffer.putInt(pos);

			switch (operator) {
			case ReadBase.operator:
				readFeatureBuffer.put(reader.baseCodec.readData());
				readFeatureBuffer.put(reader.qualityScoreCodec.readData());
				break;
			case Substitution.operator:
				readFeatureBuffer.put(reader.baseSubstitutionCodec.readData());
				break;
			case Insertion.operator:
				byte[] ins = reader.insertionCodec.readData();
				readFeatureBuffer.putInt(ins.length);
				readFeatureBuffer.put(ins);
				break;
			case SoftClip.operator:
				byte[] softClip = reader.softClipCodec.readData();
				readFeatureBuffer.putInt(softClip.length);
				readFeatureBuffer.put(softClip);
				break;
			case HardClip.operator:
				readFeatureBuffer.putInt(reader.hardClipCodec.readData());
				break;
			case Padding.operator:
				readFeatureBuffer.putInt(reader.paddingCodec.readData());
				break;
			case Deletion.operator:
				readFeatureBuffer.putInt(reader.deletionLengthCodec.readData());
				break;
			case RefSkip.operator:
				readFeatureBuffer.putInt(reader.refSkipCodec.readData());
				break;
			case InsertBase.operator:
				readFeatureBuffer.put(reader.baseCodec.readData());
				break;
			case BaseQualityScore.operator:
				readFeatureBuffer.put(reader.qualityScoreCodec.readData());
				break;
			case Bases.operator:
				readFeatureBuffer.put(reader.basesCodec.readData());
				break;
			case Scores.operator:
				readFeatureBuffer.put(reader.scoresCodec.readData());
				break;
			default:
				throw new RuntimeException("Unknown read feature operator: " + operator);
			}
		}
		readFeatureBuffer.flip();
	}

	public final void restoreReadBases(int readLength, int prevAlStart, byte[] ref,
			SubstitutionMatrix substitutionMatrix, byte[] bases) {
		readFeatureBuffer.rewind();

		int posInRead = 1;
		int alignmentStart = prevAlStart - 1;

		int posInSeq = 0;
		if (!readFeatureBuffer.hasRemaining()) {
			if (ref.length < alignmentStart + readLength) {
				Arrays.fill(bases, 0, readLength, (byte) 'N');
				System.arraycopy(ref, alignmentStart, bases, 0, Math.min(readLength, ref.length - alignmentStart));
			} else
				System.arraycopy(ref, alignmentStart, bases, 0, readLength);

		}

		for (int r = 0; r < readFeatureSize; r++) {
			byte op = readFeatureBuffer.get();
			int rfPos = readFeatureBuffer.getInt();

			// TODO: bases mapped beyond the reference:
			for (; posInRead < rfPos; posInRead++) {
				bases[posInRead - 1] = ref[alignmentStart + posInSeq++];
			}

			int len = 0;
			switch (op) {
			case Substitution.operator:
				byte refBase = ref[alignmentStart + posInSeq];
				byte base = substitutionMatrix.base(refBase, readFeatureBuffer.get());
				bases[posInRead - 1] = base;
				posInRead++;
				posInSeq++;
				break;
			case Insertion.operator:
				len = readFeatureBuffer.getInt();
				readFeatureBuffer.get(bases, posInRead - 1, len);
				posInRead += len;
				break;
			case SoftClip.operator:
				len = readFeatureBuffer.getInt();
				readFeatureBuffer.get(bases, posInRead - 1, len);
				posInRead += len;
				break;
			case HardClip.operator:
				readFeatureBuffer.getInt();
				break;
			case RefSkip.operator:
				len = readFeatureBuffer.getInt();
				posInSeq += len;
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
				readFeatureBuffer.get();
				posInSeq++;
				break;
			case BaseQualityScore.operator:
				readFeatureBuffer.get();
				break;
			default:
				throw new RuntimeException("Unkown operator: " + op);
			}
		}
		for (; posInRead <= readLength && alignmentStart + posInSeq < ref.length; posInRead++)
			bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

	}

	public final Cigar getCigar(int readLength) {
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
				readFeatureBuffer.position(readFeatureBuffer.position() + rfLen);
				break;
			case SoftClip.operator:
				co = CigarOperator.SOFT_CLIP;
				rfLen = readFeatureBuffer.getInt();
				readFeatureBuffer.position(readFeatureBuffer.position() + rfLen);
				break;
			case HardClip.operator:
				co = CigarOperator.HARD_CLIP;
				rfLen = readFeatureBuffer.getInt();
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
				readFeatureBuffer.get();
				break;
			case BaseQualityScore.operator:
				readFeatureBuffer.get();
				continue;
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
					ce = new CigarElement(readLength - (lastOpLen + lastOpPos) + 1, CigarOperator.M);
					list.add(ce);
				}
			} else if (readLength > lastOpPos - 1) {
				ce = new CigarElement(readLength - lastOpPos + 1, CigarOperator.M);
				list.add(ce);
			}
		}

		if (list.isEmpty()) {
			ce = new CigarElement(readLength, CigarOperator.M);
			return new Cigar(Arrays.asList(ce));
		}

		return new Cigar(list);
	}

	public void restoreQualityScores(int readLength, int prevAlStart, byte[] scores) {
		readFeatureBuffer.rewind();

		int posInRead = 1;

		for (int r = 0; r < readFeatureSize; r++) {
			byte op = readFeatureBuffer.get();
			posInRead = readFeatureBuffer.getInt();

			int len = 0;
			switch (op) {
			case Substitution.operator:
				readFeatureBuffer.get();
				break;
			case Insertion.operator:
				len = readFeatureBuffer.getInt();
				for (int i = 0; i < len; i++)
					readFeatureBuffer.get();
				break;
			case SoftClip.operator:
				len = readFeatureBuffer.getInt();
				for (int i = 0; i < len; i++)
					readFeatureBuffer.get();
				break;
			case HardClip.operator:
				readFeatureBuffer.getInt();
				break;
			case RefSkip.operator:
				len = readFeatureBuffer.getInt();
				break;
			case Padding.operator:
				readFeatureBuffer.getInt();
				break;
			case Deletion.operator:
				readFeatureBuffer.getInt();
				break;
			case InsertBase.operator:
				readFeatureBuffer.get();
				break;
			case ReadBase.operator:
				readFeatureBuffer.get();
				scores[posInRead - 1] = readFeatureBuffer.get();
				break;
			case BaseQualityScore.operator:
				scores[posInRead - 1] = readFeatureBuffer.get();
				break;
			default:
				throw new RuntimeException("Unkown operator: " + op);
			}
		}
	}
}
