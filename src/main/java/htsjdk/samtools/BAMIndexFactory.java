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
package htsjdk.samtools;

import java.io.File;

public class BAMIndexFactory {
	public static final BAMIndexFactory SHARED_INSTANCE = new BAMIndexFactory();

	public BAMIndex createCachingIndex(File indexFile, SAMSequenceDictionary dictionary) {
		return new CachingBAMFileIndex(indexFile, dictionary);
	}

	public long[] getBAMIndexPointers(File indexFile, SAMSequenceDictionary dictionary, String sequenceName,
			int alignmentStart, int alignmentEnd) {
		long[] filePointers = new long[0];

		final int referenceIndex = dictionary.getSequenceIndex(sequenceName);
		if (referenceIndex != -1) {
			final BAMIndex fileIndex = BAMIndexFactory.SHARED_INSTANCE.createCachingIndex(indexFile, dictionary);
			final BAMFileSpan fileSpan = fileIndex.getSpanOverlapping(referenceIndex, alignmentStart, alignmentEnd);
			filePointers = fileSpan != null ? fileSpan.toCoordinateArray() : null;
		}
		return filePointers;
	}
}
