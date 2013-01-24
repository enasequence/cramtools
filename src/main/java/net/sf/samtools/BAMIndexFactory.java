package net.sf.samtools;

import java.io.File;

public class BAMIndexFactory {
	public static final BAMIndexFactory SHARED_INSTANCE = new BAMIndexFactory();

	public BAMIndex createCachingIndex(File indexFile,
			SAMSequenceDictionary dictionary) {
		return new CachingBAMFileIndex(indexFile, dictionary);
	}

	public long[] getBAMIndexPointers(File indexFile,
			SAMSequenceDictionary dictionary, String sequenceName,
			int alignmentStart, int alignmentEnd) {
		long[] filePointers = new long[0];

		final int referenceIndex = dictionary.getSequenceIndex(sequenceName);
		if (referenceIndex != -1) {
			final BAMIndex fileIndex = BAMIndexFactory.SHARED_INSTANCE
					.createCachingIndex(indexFile, dictionary);
			final BAMFileSpan fileSpan = fileIndex.getSpanOverlapping(
					referenceIndex, alignmentStart, alignmentEnd);
			filePointers = fileSpan != null ? fileSpan.toCoordinateArray()
					: null;
		}
		return filePointers;
	}
}
