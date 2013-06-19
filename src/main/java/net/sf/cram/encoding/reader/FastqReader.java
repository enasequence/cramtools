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

import java.nio.ByteBuffer;

import net.sf.cram.structure.CramRecord;

public class FastqReader extends AbstractFastqReader {

	public ByteBuffer[] bufs;
	public boolean appendSegmentIndexToReadNames = true ;

	public FastqReader() {
		bufs = new ByteBuffer[3];
		for (int i = 0; i < bufs.length; i++)
			bufs[i] = ByteBuffer.allocate(1024 * 1024 * 10);
	}

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
		if (scores != null) {
			for (int i = 0; i < readLength; i++)
				scores[i] += 33;
			buf.put(scores, 0, readLength);
		}
		buf.put((byte) '\n');
	}
}
