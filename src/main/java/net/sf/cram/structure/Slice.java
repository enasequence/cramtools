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
package net.sf.cram.structure;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import net.sf.cram.common.Utils;
import net.sf.picard.util.Log;

public class Slice {
	private static final Log log = Log.getInstance(Slice.class);

	// as defined in the specs:
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;
	public int nofRecords = -1;
	public long globalRecordCounter = -1;
	public int nofBlocks = -1;
	public int[] contentIDs;
	public int embeddedRefBlockContentID = -1;
	public byte[] refMD5;

	// content associated with ids:
	public Block headerBlock;
	public BlockContentType contentType;
	public Block coreBlock;
	public Block embeddedRefBlock;
	public Map<Integer, Block> external;

	// for indexing purposes:
	public int offset = -1;
	public long containerOffset = -1;
	public int size = -1;
	public int index = -1;

	// to pass this to the container:
	public long bases;

	private void alignmentBordersSanityCheck(byte[] ref) {
		if (alignmentStart > 0 && sequenceId >= 0 && ref == null)
			throw new NullPointerException("Mapped slice reference is null.");

		if (alignmentStart > ref.length) {
			log.error(String.format("Slice mapped outside of reference: seqid=%d, alstart=%d, counter=%d.", sequenceId,
					alignmentStart, globalRecordCounter));
			throw new RuntimeException("Slice mapped outside of the reference.");
		}

		if (alignmentStart + alignmentSpan > ref.length) {
			log.warn(String.format(
					"Slice partially mapped outside of reference: seqid=%d, alstart=%d, alspan=%d, counter=%d.",
					sequenceId, alignmentStart, alignmentSpan, globalRecordCounter));
		}
	}

	public boolean validateRefMD5(byte[] ref) throws NoSuchAlgorithmException {
		alignmentBordersSanityCheck(ref);

		int span = Math.min(alignmentSpan, ref.length - alignmentStart + 1);
		String md5 = Utils.calculateMD5String(ref, alignmentStart - 1, span);
		String sliceMD5 = String.format("%032x", new BigInteger(1, refMD5));
		if (!md5.equals(sliceMD5)) {
			StringBuffer sb = new StringBuffer();
			int shoulder = 10;
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart - 1, alignmentStart + shoulder)));
			sb.append("...");
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart - 1 + span - shoulder, alignmentStart + span)));

			log.error(String.format("Slice md5 %s does not match calculated %s, %d:%d-%d, %s", sliceMD5, md5,
					sequenceId, alignmentStart, span, sb.toString()));
			return false;
		}
		return true;
	}

	public void setRefMD5(byte[] ref) {
		alignmentBordersSanityCheck(ref);

		if (sequenceId < 0 && alignmentStart < 1) {
			refMD5 = new byte[16];
			Arrays.fill(refMD5, (byte) 0);

			log.debug("Empty slice ref md5 is set.");
		} else {

			int span = Math.min(alignmentSpan, ref.length - alignmentStart + 1);

			if (alignmentStart + span > ref.length + 1)
				throw new RuntimeException("Invalid alignment boundaries.");

			refMD5 = Utils.calculateMD5(ref, alignmentStart - 1, span);

			StringBuffer sb = new StringBuffer();
			int shoulder = 10;
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart - 1, alignmentStart + shoulder)));
			sb.append("...");
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart - 1 + span - shoulder, alignmentStart + span)));

			log.debug(String.format("Slice md5: %s for %d:%d-%d, %s",
					String.format("%032x", new BigInteger(1, refMD5)), sequenceId, alignmentStart, alignmentStart
							+ span - 1, sb.toString()));
		}
	}
}
