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

import java.nio.ByteBuffer;

public class ReaderToFastq extends AbstractFastqReader {

	public static final int BUF_SIZE = 1024 * 1024 * 10;

	public ByteBuffer[] bufs;

	public ReaderToFastq() {
		this(BUF_SIZE);
	}

	public ReaderToFastq(int bufSize) {
		bufs = new ByteBuffer[3];
		for (int i = 0; i < bufs.length; i++)
			bufs[i] = ByteBuffer.allocate(bufSize);
	}

	@Override
	protected void writeRead(byte[] name, int flags, byte[] bases, byte[] scores) {
		int indexInTemplate = getSegmentIndexInTemplate(flags);
		ByteBuffer buf = bufs[indexInTemplate];

		buf.put((byte) '@');
		buf.put(readName);
		if (appendSegmentIndexToReadNames && indexInTemplate > 0) {
			buf.put((byte) '/');
			byte segmentIndex = (byte) (48 + indexInTemplate);
			buf.put(segmentIndex);
		}
		buf.put((byte) '\n');
		buf.put(bases, 0, readLength);
		buf.put((byte) '\n');
		buf.put((byte) '+');
		buf.put((byte) '\n');
		if (scores != null)
			buf.put(scores, 0, readLength);
		else
			for (int i = 0; i < readLength; i++)
				buf.put((byte) 33);

		buf.put((byte) '\n');
	}

	@Override
	public void finish() {

	}

	@Override
	protected byte[] refSeqChanged(int seqID) {
		throw new RuntimeException("not implemented.");
	}
}
