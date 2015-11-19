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

import htsjdk.samtools.cram.structure.ReadTag;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A reader that only keeps track of alignment spans. The intended use is for
 * CRAI index.
 * 
 * @author vadim
 *
 */
public class RefSeqIdReader extends AbstractReader {
	private ReadFeatureBuffer rfBuf = new ReadFeatureBuffer();

	private int flags;
	private int compressionFlags;
	private int readLength;

	private int recordCounter = 0;
	private Map<Integer, Span> spans = new HashMap<Integer, Span>();

	private int globalReferenceSequenceId;

	private int alignmentStart;

	public RefSeqIdReader(int seqId, int alignmentStart) {
		super();
		this.globalReferenceSequenceId = seqId;
		this.alignmentStart = alignmentStart;
	}

	public Map<Integer, Span> getReferenceSpans() {
		return spans;
	}

	public static class Span {
		public int start, span;

		Span(int start, int span) {
			this.start = start;
			this.span = span;
		}

		void add(int start, int span) {
			if (this.start > start) {
				this.span = Math.max(this.start + this.span, start + span) - start;
				this.start = start;
			} else if (this.start < start) {
				this.span = Math.max(this.start + this.span, start + span) - this.start;
			} else
				this.span = Math.max(this.span, span);
		}
	}

	public void read() throws IOException {
		try {
			flags = bitFlagsCodec.readData();

			compressionFlags = compressionBitFlagsCodec.readData();

			int seqId = globalReferenceSequenceId;
			if (refId == -2) {
				seqId = refIdCodec.readData();
			}

			int len = readLengthCodec.readData();
			if (APDelta)
				alignmentStart += alignmentStartCodec.readData();
			else
				alignmentStart = alignmentStartCodec.readData();

			if (!spans.containsKey(seqId)) {
				spans.put(seqId, new Span(alignmentStart, len));
			} else
				spans.get(seqId).add(alignmentStart, len);

			readGroupCodec.readData();

			if (captureReadNames)
				readNameCodec.readData();

			// mate record:
			if ((compressionFlags & CramFlags.DETACHED_FLAG) != 0) {
				mateBitFlagCodec.readData();
				if (!captureReadNames)
					readNameCodec.readData();

				mateReferenceIdCodec.readData();
				mateAlignmentStartCodec.readData();
				insertSizeCodec.readData();
				detachedCount++;
			} else if ((compressionFlags & CramFlags.HAS_MATE_DOWNSTREAM_FLAG) != 0) {
				distanceToNextFragmentCodec.readData();
			}

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

			if ((flags & CramFlags.SEGMENT_UNMAPPED_FLAG) == 0) {
				rfBuf.readReadFeatures(this);
				mappingScoreCodec.readData();
			} else {
				for (int i = 0; i < readLength; i++)
					baseCodec.readData();
			}

			if ((compressionFlags & CramFlags.FORCE_PRESERVE_QS_FLAG) != 0) {
				for (int i = 0; i < readLength; i++)
					qualityScoreCodec.readData();
			}

			recordCounter++;
		} catch (Exception e) {
			System.err.printf("Failed at record %d. \n", recordCounter);
			throw new RuntimeException(e);
		}
	}
}
