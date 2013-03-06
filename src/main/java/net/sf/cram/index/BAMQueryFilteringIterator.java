package net.sf.cram.index;

import java.util.NoSuchElementException;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.CloseableIterator;

public class BAMQueryFilteringIterator implements CloseableIterator<SAMRecord> {
	/**
	 * A decorating iterator that filters out records that are outside the
	 * bounds of the given query parameters.
	 */
	public enum QueryType {
		CONTAINED, OVERLAPPING, STARTING_AT
	};

	/**
	 * The wrapped iterator.
	 */
	private final CloseableIterator<SAMRecord> wrappedIterator;

	/**
	 * The next record to be returned. Will be null if no such record exists.
	 */
	private SAMRecord mNextRecord;

	private final int mReferenceIndex;
	private final int mRegionStart;
	private final int mRegionEnd;
	private final QueryType mQueryType;
	private boolean isClosed = false;

	public BAMQueryFilteringIterator(final CloseableIterator<SAMRecord> iterator,
			final String sequence, final int start, final int end,
			final QueryType queryType, SAMFileHeader fileHeader) {
		this.wrappedIterator = iterator;
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
