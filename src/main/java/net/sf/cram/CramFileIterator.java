package net.sf.cram;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.util.CloseableIterator;
import net.sf.samtools.util.RuntimeEOFException;

public class CramFileIterator implements CloseableIterator<SAMRecord> {
	private static Log log = Log.getInstance(CramFileIterator.class);
	private InputStream is;
	private CramHeader cramHeader;
	private ArrayList<SAMRecord> records;
	private int recordCounter = 0;
	private SAMRecord nextRecord = null;
	private ReferenceSequenceFile referenceSequenceFile;;

	public CramFileIterator(InputStream is,
			ReferenceSequenceFile referenceSequenceFile) throws IOException {
		this.is = is;
		this.referenceSequenceFile = referenceSequenceFile;
		cramHeader = ReadWrite.readCramHeader(is);
		records = new ArrayList<>(100000) ;
	}
	
	public CramHeader getCramHeader() {
		return cramHeader;
	}

	private void nextContainer() throws IOException, IllegalArgumentException,
			IllegalAccessException {
		if (records == null)
			records = new ArrayList<>(100000);
		records.clear();
		recordCounter = 0;

		Container c = null;
		try {
			c = ReadWrite.readContainer(cramHeader.samFileHeader, is);
		} catch (EOFException e) {
			return;
		}

		List<CramRecord> cramRecords = null;
		try {
			cramRecords = BLOCK_PROTO.records(c.h, c, cramHeader.samFileHeader);
		} catch (EOFException e) {
			throw e;
		}
		SAMSequenceRecord sequence = cramHeader.samFileHeader
				.getSequence(c.sequenceId);
		ReferenceSequence referenceSequence = referenceSequenceFile
				.getSequence(sequence.getSequenceName());
		byte[] ref = referenceSequence.getBases();

		long time1 = System.nanoTime();
		CramNormalizer n = new CramNormalizer(cramHeader.samFileHeader, ref,
				c.alignmentStart);
		n.normalize(cramRecords, true);
		long time2 = System.nanoTime();

		Cram2BamRecordFactory c2sFactory = new Cram2BamRecordFactory(
				cramHeader.samFileHeader);

		long c2sTime = 0;

		for (CramRecord r : cramRecords) {
			long time = System.nanoTime();
			SAMRecord s = c2sFactory.create(r);
			c2sTime += System.nanoTime() - time;
			records.add(s);
		}
		log.info(String.format(
				"CONTAINER READ: io %dms, parse %dms, norm %dms, convert %dms",
				c.readMS / 1000000, c.parseMS / 1000000, c2sTime / 1000000,
				(time2 - time1) / 1000000));
	}

	@Override
	public boolean hasNext() {
		if (recordCounter + 1 >= records.size()) {
			try {
				nextContainer();
				if (records.isEmpty()) return false ;
			} catch (Exception e) {
				throw new RuntimeEOFException(e);
			}
		}

		nextRecord = records.get(recordCounter++);
		return true;
	}

	@Override
	public SAMRecord next() {
		return nextRecord;
	}

	@Override
	public void remove() {
		throw new RuntimeException("Removal of records not implemented.");
	}

	@Override
	public void close() {
		records.clear();
		try {
			is.close();
		} catch (IOException e) {
		}
	}

}
