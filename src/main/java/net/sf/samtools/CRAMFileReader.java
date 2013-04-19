package net.sf.samtools;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.samtools.BAMFileReader.QueryType;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.CloseableIterator;
import net.sf.samtools.util.RuntimeEOFException;
import net.sf.samtools.util.SeekableFileStream;
import net.sf.samtools.util.SeekableStream;

public class CRAMFileReader extends SAMFileReader.ReaderImplementation {
	private File file;
	private ReferenceSequenceFile referenceSequenceFile;
	private CramHeader header;
	private InputStream is;
	private SAMRecordIterator it;
	private BAMIndex mIndex;
	private File mIndexFile;
	private boolean mEnableIndexCaching;
	private File mIndexStream;
	private boolean mEnableIndexMemoryMapping;

	private ValidationStringency validationStringency;

	public CRAMFileReader(File file, InputStream is,
			ReferenceSequenceFile referenceSequenceFile) {
		this.file = file;
		this.is = is;
		this.referenceSequenceFile = referenceSequenceFile;

		if (file == null)
			getIterator();
	}

	public CRAMFileReader(File bamFile, File indexFile,
			ReferenceSequenceFile referenceSequenceFile) {
		this.file = bamFile;
		this.mIndexFile = indexFile;
		this.referenceSequenceFile = referenceSequenceFile;

		if (file == null)
			getIterator();
	}

	public SAMRecordIterator iterator() {
		return getIterator();
	}

	private void readHeader() throws FileNotFoundException, IOException {
		header = ReadWrite.readCramHeader(new FileInputStream(file));
	}

	@Override
	void enableFileSource(SAMFileReader reader, boolean enabled) {
		// throw new RuntimeException("Not implemented.");
	}

	@Override
	void enableIndexCaching(boolean enabled) {
		// throw new RuntimeException("Not implemented.");
	}

	@Override
	void enableIndexMemoryMapping(boolean enabled) {
		// throw new RuntimeException("Not implemented.");
	}

	@Override
	void enableCrcChecking(boolean enabled) {
		// throw new RuntimeException("Not implemented.");
	}

	@Override
	void setSAMRecordFactory(SAMRecordFactory factory) {
	}

	@Override
	boolean hasIndex() {
		return mIndex != null || mIndexFile != null || mIndexStream != null;
	}

	@Override
	BAMIndex getIndex() {
		if (!hasIndex())
			throw new SAMException("No index is available for this BAM file.");
		if (mIndex == null) {
			if (mIndexFile != null)
				mIndex = mEnableIndexCaching ? new CachingBAMFileIndex(
						mIndexFile, getFileHeader().getSequenceDictionary(),
						mEnableIndexMemoryMapping) : new DiskBasedBAMFileIndex(
						mIndexFile, getFileHeader().getSequenceDictionary(),
						mEnableIndexMemoryMapping);
			else
				mIndex = mEnableIndexCaching ? new CachingBAMFileIndex(
						mIndexStream, getFileHeader().getSequenceDictionary())
						: new DiskBasedBAMFileIndex(mIndexStream,
								getFileHeader().getSequenceDictionary());
		}
		return mIndex;
	}

	@Override
	SAMFileHeader getFileHeader() {
		try {
			if (header == null)
				readHeader();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return header.samFileHeader;
	}

	@Override
	SAMRecordIterator getIterator() {
		if (it != null && file == null)
			return it;
		try {
			SAMIterator si = null;
			if (file != null)
				si = new SAMIterator(new FileInputStream(file),
						referenceSequenceFile);
			else
				si = new SAMIterator(is, referenceSequenceFile);

			si.setValidationStringency(validationStringency);
			header = si.getCramHeader();
			it = si;
			return it;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	CloseableIterator<SAMRecord> getIterator(SAMFileSpan fileSpan) {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	SAMFileSpan getFilePointerSpanningReads() {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public CloseableIterator<SAMRecord> query(String sequence, int start, int end,
			boolean contained) {
		CloseableIterator<SAMRecord> iterator = queryAlignmentStart(sequence,
				start);

		QueryType qt = QueryType.CONTAINED;
		if (!contained)
			qt = QueryType.OVERLAPPING;
		if (end == -1)
			qt = QueryType.STARTING_AT;
		return new BAMQueryFilteringIterator(iterator, sequence, start, end, qt);
	}

	private static SAMRecordIterator emptyIterator = new SAMRecordIterator() {

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public SAMRecord next() {
			throw new RuntimeException("No records.");
		}

		@Override
		public void remove() {
			throw new RuntimeException("Remove not supported.");
		}

		@Override
		public void close() {
		}

		@Override
		public SAMRecordIterator assertSorted(SortOrder sortOrder) {
			// TODO Auto-generated method stub
			return null;
		}
	};

	@Override
	public CloseableIterator<SAMRecord> queryAlignmentStart(String sequence,
			int start) {
		long[] filePointers = null;

		// Hit the index to determine the chunk boundaries for the required
		// data.
		final SAMFileHeader fileHeader = getFileHeader();
		final int referenceIndex = fileHeader.getSequenceIndex(sequence);
		if (referenceIndex != -1) {
			final BAMIndex fileIndex = getIndex();
			final BAMFileSpan fileSpan = fileIndex.getSpanOverlapping(
					referenceIndex, start, -1);
			filePointers = fileSpan != null ? fileSpan.toCoordinateArray()
					: null;
		}

		if (filePointers == null || filePointers.length == 0)
			return emptyIterator;

		SeekableStream s = null;
		if (file != null) {
			try {
				s = new SeekableFileStream(file);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else if (is instanceof SeekableStream)
			s = (SeekableStream) is;

		SAMIterator si = null;
		try {
			s.seek(0);
			si = new SAMIterator(s, referenceSequenceFile);
			si.setValidationStringency(validationStringency);
			it = si;
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}

		Container c = null;
		for (int i = 0; i < filePointers.length; i += 2) {
			long containerOffset = filePointers[i] >>> 16;
			int sliceIndex = (int) ((filePointers[i] << 48) >>> 48);
			try {
				s.seek(containerOffset);
				// the following is not optimal because this is container-level
				// access, not slice:

				// CountingInputStream cis = new CountingInputStream(s) ;
				c = ReadWrite.readContainerHeader(s);
				// long headerSize = cis.getCount() ;
				// int sliceOffset = c.landmarks[sliceIndex] ;
				if (c.alignmentStart + c.alignmentSpan > start) {
					s.seek(containerOffset);
					// s.seek(containerOffset + headerSize + sliceOffset);
					return si;
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		it = emptyIterator;
		return it;
	}

	@Override
	public CloseableIterator<SAMRecord> queryUnmapped() {
		final long startOfLastLinearBin = getIndex().getStartOfLastLinearBin();

		SeekableStream s = null;
		if (file != null) {
			try {
				s = new SeekableFileStream(file);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else if (is instanceof SeekableStream)
			s = (SeekableStream) is;

		SAMIterator si = null;
		try {
			s.seek(0);
			si = new SAMIterator(s, referenceSequenceFile);
			si.setValidationStringency(validationStringency);
			s.seek(startOfLastLinearBin);
			it = si;
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}

		return it;
	}

	@Override
	void close() {
		if (it != null)
			it.close();
		if (is != null)
			try {
				is.close();
			} catch (IOException e) {
			}

		if (mIndex != null)
			mIndex.close();
	}

	@Override
	void setValidationStringency(ValidationStringency validationStringency) {
		this.validationStringency = validationStringency;
	}

	@Override
	ValidationStringency getValidationStringency() {
		return validationStringency;
	}

	public static boolean isCRAMFile(File file) throws IOException {
		final int buffSize = CramHeader.magick.length;
		FileInputStream fis = new FileInputStream(file);
		DataInputStream dis = new DataInputStream(fis);
		final byte[] buffer = new byte[buffSize];
		dis.readFully(buffer);
		dis.close();

		return Arrays.equals(buffer, CramHeader.magick);
	}

	/**
	 * A decorating iterator that filters out records that are outside the
	 * bounds of the given query parameters.
	 */
	private class BAMQueryFilteringIterator implements
			CloseableIterator<SAMRecord> {
		/**
		 * The wrapped iterator.
		 */
		private final CloseableIterator<SAMRecord> wrappedIterator;

		/**
		 * The next record to be returned. Will be null if no such record
		 * exists.
		 */
		private SAMRecord mNextRecord;

		private final int mReferenceIndex;
		private final int mRegionStart;
		private final int mRegionEnd;
		private final QueryType mQueryType;
		private boolean isClosed = false;

		public BAMQueryFilteringIterator(
				final CloseableIterator<SAMRecord> iterator,
				final String sequence, final int start, final int end,
				final QueryType queryType) {
			this.wrappedIterator = iterator;
			final SAMFileHeader fileHeader = getFileHeader();
			mReferenceIndex = fileHeader.getSequenceIndex(sequence);
			mRegionStart = start;
			if (queryType == QueryType.STARTING_AT) {
				mRegionEnd = mRegionStart;
			} else {
				mRegionEnd = (end <= 0) ? Integer.MAX_VALUE : end;
			}
			mQueryType = queryType;
			mNextRecord = advance();
		}

		/**
		 * Returns true if a next element exists; false otherwise.
		 */
		public boolean hasNext() {
			if (isClosed)
				throw new IllegalStateException("Iterator has been closed");
			return mNextRecord != null;
		}

		/**
		 * Gets the next record from the given iterator.
		 * 
		 * @return The next SAM record in the iterator.
		 */
		public SAMRecord next() {
			if (!hasNext())
				throw new NoSuchElementException(
						"BAMQueryFilteringIterator: no next element available");
			final SAMRecord currentRead = mNextRecord;
			mNextRecord = advance();
			return currentRead;
		}

		/**
		 * Closes down the existing iterator.
		 */
		public void close() {
			isClosed = true;
		}

		/**
		 * @throws UnsupportedOperationException
		 *             always.
		 */
		public void remove() {
			throw new UnsupportedOperationException("Not supported: remove");
		}

		SAMRecord advance() {
			while (true) {
				// Pull next record from stream
				if (!wrappedIterator.hasNext())
					return null;

				final SAMRecord record = wrappedIterator.next();
				// If beyond the end of this reference sequence, end iteration
				final int referenceIndex = record.getReferenceIndex();
				if (referenceIndex != mReferenceIndex) {
					if (referenceIndex < 0 || referenceIndex > mReferenceIndex) {
						return null;
					}
					// If before this reference sequence, continue
					continue;
				}
				if (mRegionStart == 0 && mRegionEnd == Integer.MAX_VALUE) {
					// Quick exit to avoid expensive alignment end calculation
					return record;
				}
				final int alignmentStart = record.getAlignmentStart();
				// If read is unmapped but has a coordinate, return it if the
				// coordinate is within
				// the query region, regardless of whether the mapped mate will
				// be returned.
				final int alignmentEnd;
				if (mQueryType == QueryType.STARTING_AT) {
					alignmentEnd = -1;
				} else {
					alignmentEnd = (record.getAlignmentEnd() != SAMRecord.NO_ALIGNMENT_START ? record
							.getAlignmentEnd() : alignmentStart);
				}

				if (alignmentStart > mRegionEnd) {
					// If scanned beyond target region, end iteration
					return null;
				}
				// Filter for overlap with region
				if (mQueryType == QueryType.CONTAINED) {
					if (alignmentStart >= mRegionStart
							&& alignmentEnd <= mRegionEnd) {
						return record;
					}
				} else if (mQueryType == QueryType.OVERLAPPING) {
					if (alignmentEnd >= mRegionStart
							&& alignmentStart <= mRegionEnd) {
						return record;
					}
				} else {
					if (alignmentStart == mRegionStart) {
						return record;
					}
				}
			}
		}
	}
}
