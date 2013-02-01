package net.sf.cram.select;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMFileHeader.SortOrder;

class BoundSAMRecordIterator implements SAMRecordIterator {
	private SAMRecordIterator delegate;
	private long firstRecord = 0, lastRecord = Long.MAX_VALUE;
	private long counter = 0;

	public BoundSAMRecordIterator(SAMRecordIterator delegate,
			long firstRecord, long lastRecord) {
		this.delegate = delegate;
		this.firstRecord = firstRecord;
		this.lastRecord = lastRecord;
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public boolean hasNext() {
		while (delegate.hasNext() && counter < firstRecord) {
			next();
		}
		return counter < lastRecord && delegate.hasNext() ;
	}

	@Override
	public SAMRecord next() {
		counter++;
		return delegate.next();
	}

	@Override
	public void remove() {
		delegate.remove();
	}

	@Override
	public SAMRecordIterator assertSorted(SortOrder sortOrder) {
		return delegate.assertSorted(sortOrder);
	}
	
}