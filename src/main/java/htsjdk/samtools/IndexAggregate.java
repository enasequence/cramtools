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

package htsjdk.samtools;

import htsjdk.samtools.cram.io.CramInt;
import htsjdk.samtools.cram.io.ITF8;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import net.sf.cram.index.CramIndex;
import net.sf.cram.index.CramIndex.Entry;

/**
 * A utility class to hide details of BAI/CRAI indexes. It will try and find a
 * matching index file, open it and seek the data stream to the query position.
 * 
 * @author vadim
 * 
 */
public class IndexAggregate {
	private static Log log = Log.getInstance(IndexAggregate.class);
	private BAMIndex bai;
	private List<CramIndex.Entry> crai;

	public static IndexAggregate forDataFile(SeekableStream stream, SAMSequenceDictionary dictionary)
			throws IOException {
		String path = stream.getSource();
		File indexFile = findIndexFileFor(path);
		if (indexFile == null)
			throw new FileNotFoundException("No index found for file: " + path);

		log.info("Using index file: " + indexFile.getAbsolutePath());
		IndexAggregate a = new IndexAggregate();
		if (indexFile.getName().matches("(?i).*\\.bai")) {
			a.bai = new CachingBAMFileIndex(indexFile, dictionary);
			return a;
		}
		if (indexFile.getName().matches("(?i).*\\.crai")) {
			a.crai = CramIndex.readIndex(new GZIPInputStream(new FileInputStream(indexFile)));
			return a;
		}

		throw new FileNotFoundException("No index found for file: " + path);
	}

	public static IndexAggregate fromBaiFile(SeekableStream baiStream, SAMSequenceDictionary dictionary)
			throws IOException {
		IndexAggregate a = new IndexAggregate();
		a.bai = new CachingBAMFileIndex(baiStream, dictionary);
		return a;
	}

	public static IndexAggregate fromCraiFile(InputStream craiStream, SAMSequenceDictionary dictionary)
			throws IOException {
		IndexAggregate a = new IndexAggregate();
		a.crai = CramIndex.readIndex(new GZIPInputStream(craiStream));
		return a;
	}

	/**
	 * Find and seek the data stream to the position of the alignment query.
	 * 
	 * @param seqId
	 *            reference sequence id
	 * @param start
	 *            alignment start, 1-based inclusive
	 * @param end
	 *            alignment end, 1-based exclusive
	 * @param cramStream
	 *            the data stream to seek in
	 * @return the offset found or -1 if the query was not found
	 * @throws IOException
	 */
	public long seek(int seqId, int start, int end, SeekableStream cramStream) throws IOException {
		if (crai != null)
			return seek(crai, seqId, start, end, cramStream);
		if (bai != null)
			return seek(bai, seqId, start, end, cramStream);
		return -1;
	}

	private static long seek(List<CramIndex.Entry> index, int seqId, int start, int end, SeekableStream cramStream)
			throws IOException {
		List<Entry> found = CramIndex.find(index, seqId, start, end - start + 1);
		if (found == null || found.size() == 0)
			return -1;
		cramStream.seek(found.get(0).containerStartOffset);
		log.debug("Found query at offset: " + found.get(0).containerStartOffset);
		return found.get(0).containerStartOffset;
	}

	private static long seek(BAMIndex index, int seqId, int start, int end, SeekableStream cramStream)
			throws IOException {
		BAMFileSpan span = index.getSpanOverlapping(seqId, start, end);
		if (span == null)
			return -1;
		long[] coords = span.toCoordinateArray();
		if (coords.length == 0)
			return -1;
		long[] offsets = new long[coords.length / 2];
		for (int i = 0; i < offsets.length; i++) {
			offsets[i] = coords[i * 2] >> 16;
		}
		Arrays.sort(offsets);

		// peek into container in offset ascending order and choose the first
		// that intersects the query:
		for (int i = 0; i < offsets.length; i++) {
			log.debug("Peeking at offset: " + offsets[i]);
			IndexAggregate.ContainerBoundary b = peek(cramStream, offsets[i]);
			if (b == null)
				continue;

			boolean intersects = intersects(start, end, b);
			// System.out.printf("%b, %d, %d, %d, %d\n", intersects, start, end,
			// b.start, b.start + b.span);

			if (intersects(start, end, b)) {
				long offset = offsets[i];
				log.debug("Found query at offset: " + offset);
				cramStream.seek(offset);
				return offset;
			}

		}
		return -1;
	}

	private static File findIndexFileFor(String path) {
		for (String candide : indexCandidates(path)) {
			File indexFile = new File(candide);
			if (indexFile.isFile())
				return indexFile;
		}
		return null;
	}

	/**
	 * Lists possible candidates for index path given a data source path.
	 * 
	 * @param source
	 *            BAM or CRAM file path
	 * @return a list of paths possibly pointing to the index file for the
	 *         source data.
	 */
	private static List<String> indexCandidates(String source) {
		List<String> candidates = new ArrayList<String>();
		if (source.matches(".*(?i)\\.bam$")) {
			candidates.add(source + ".bai");
			candidates.add(source + ".BAI");

			candidates.add(source.replaceFirst("(?i)\\.bam$", ".bai"));
			candidates.add(source.replaceFirst("(?i)\\.bam$", ".BAI"));
		}

		if (source.matches(".*(?i)\\.cram[23]?$")) {
			candidates.add(source + ".bai");
			candidates.add(source + ".crai");

			candidates.add(source + ".CRAI");
			candidates.add(source + ".BAI");

			candidates.add(source.replaceFirst("(?i)\\.cram[23]?$", ".crai"));
			candidates.add(source.replaceFirst("(?i)\\.cram[23]?$", ".CRAI"));

			candidates.add(source.replaceFirst("(?i)\\.cram[23]?$", ".bai"));
			candidates.add(source.replaceFirst("(?i)\\.cram[23]?$", ".BAI"));
		}
		return candidates;
	}

	private static boolean intersects(int start, int end, IndexAggregate.ContainerBoundary cb) {
		final int a = start;
		final int b = end;
		final int x = cb.start;
		final int y = cb.span + x - 1;

		if (Math.max(a, b) < Math.min(x, y))
			return false;
		if (Math.max(x, y) < Math.min(a, b))
			return false;
		return true;
	}

	private static class ContainerBoundary {
		int seqId;
		int start;
		int span;
	}

	private static ContainerBoundary peek(SeekableStream stream, long offset) throws IOException {
		stream.seek(offset);
		final byte[] peek = new byte[4];
		int character = stream.read();
		if (character == -1)
			return null;

		peek[0] = (byte) character;
		for (int i = 1; i < peek.length; i++) {
			character = stream.read();
			if (character == -1)
				throw new RuntimeException("Incomplete or broken stream.");
			peek[i] = (byte) character;
		}

		IndexAggregate.ContainerBoundary b = new ContainerBoundary();
		int containerByteSize = CramInt.int32(peek);
		b.seqId = ITF8.readUnsignedITF8(stream);
		b.start = ITF8.readUnsignedITF8(stream);
		b.span = ITF8.readUnsignedITF8(stream);
		int nofRecords = ITF8.readUnsignedITF8(stream);
		return b;
	}
}