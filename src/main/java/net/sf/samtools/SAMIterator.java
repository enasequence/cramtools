package net.sf.samtools;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.cram.BLOCK_PROTO;
import net.sf.cram.Cram2BamRecordFactory;
import net.sf.cram.CramNormalizer;
import net.sf.cram.CramRecord;
import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.Utils;
import net.sf.cram.structure.Container;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.RuntimeEOFException;

public class SAMIterator implements SAMRecordIterator {
	private static Log log = Log.getInstance(SAMIterator.class);
	private InputStream is;
	private CramHeader cramHeader;
	private ArrayList<SAMRecord> records;
	private int recordCounter = 0;
	private SAMRecord nextRecord = null;
	private ReferenceSequenceFile referenceSequenceFile;
	private boolean restoreNMTag = true;
	private boolean restoreMDTag = false;
	private CramNormalizer normalizer;
	private byte[] refs;
	private int prevSeqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
	private Container container;

	private ValidationStringency validationStringency = ValidationStringency.SILENT;

	public ValidationStringency getValidationStringency() {
		return validationStringency;
	}

	public void setValidationStringency(
			ValidationStringency validationStringency) {
		this.validationStringency = validationStringency;
	}

	private long samRecordIndex;

	public SAMIterator(InputStream is,
			ReferenceSequenceFile referenceSequenceFile) throws IOException {
		this.is = is;
		this.referenceSequenceFile = referenceSequenceFile;
		cramHeader = ReadWrite.readCramHeader(is);
		records = new ArrayList<SAMRecord>(100000);
		normalizer = new CramNormalizer(cramHeader.samFileHeader);
	}

	public CramHeader getCramHeader() {
		return cramHeader;
	}

	private void nextContainer() throws IOException, IllegalArgumentException,
			IllegalAccessException {
		if (records == null)
			records = new ArrayList<SAMRecord>(100000);
		records.clear();
		recordCounter = 0;

		container = null;
		container = ReadWrite.readContainer(cramHeader.samFileHeader, is);
		if (container == null)
			return;

		ArrayList<CramRecord> cramRecords = new ArrayList<CramRecord>();
		try {
			cramRecords.clear();
			BLOCK_PROTO.getRecords(container.h, container,
					cramHeader.samFileHeader, cramRecords);
		} catch (EOFException e) {
			throw e;
		}

		if (container.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			refs = new byte[] {};
		} else if (prevSeqId < 0 || prevSeqId != container.sequenceId) {
			SAMSequenceRecord sequence = cramHeader.samFileHeader
					.getSequence(container.sequenceId);
			ReferenceSequence referenceSequence = Utils
					.trySequenceNameVariants(referenceSequenceFile,
							sequence.getSequenceName());
			refs = referenceSequence.getBases();
			prevSeqId = container.sequenceId;
		}

		long time1 = System.nanoTime();

		normalizer.normalize(cramRecords, true, refs, container.alignmentStart,
				container.h.substitutionMatrix, container.h.AP_seriesDelta);
		long time2 = System.nanoTime();

		Cram2BamRecordFactory c2sFactory = new Cram2BamRecordFactory(
				cramHeader.samFileHeader);

		long c2sTime = 0;

		for (CramRecord r : cramRecords) {
			long time = System.nanoTime();
			SAMRecord s = c2sFactory.create(r);
			c2sTime += System.nanoTime() - time;
			if (!r.isSegmentUnmapped())
				Utils.calculateMdAndNmTags(s, refs, restoreMDTag, restoreNMTag);

			s.setValidationStringency(validationStringency);

			if (validationStringency != ValidationStringency.SILENT) {
				final List<SAMValidationError> validationErrors = s.isValid();
				SAMUtils.processValidationErrors(validationErrors,
						samRecordIndex, validationStringency);
			}

			records.add(s);
		}

		log.info(String.format(
				"CONTAINER READ: io %dms, parse %dms, norm %dms, convert %dms",
				container.readTime / 1000000, container.parseTime / 1000000,
				c2sTime / 1000000, (time2 - time1) / 1000000));
	}

	@Override
	public boolean hasNext() {
		if (recordCounter >= records.size()) {
			try {
				nextContainer();
				if (records.isEmpty())
					return false;
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

	public static class CramFileIterable implements Iterable<SAMRecord> {
		private ReferenceSequenceFile referenceSequenceFile;
		private File cramFile;
		private ValidationStringency validationStringency;

		public CramFileIterable(File cramFile,
				ReferenceSequenceFile referenceSequenceFile, ValidationStringency validationStringency) {
			this.referenceSequenceFile = referenceSequenceFile;
			this.cramFile = cramFile;
			this.validationStringency = validationStringency;
			
		}
		
		public CramFileIterable(File cramFile,
				ReferenceSequenceFile referenceSequenceFile) {
			this (cramFile, referenceSequenceFile, ValidationStringency.DEFAULT_STRINGENCY) ;
		}

		@Override
		public Iterator<SAMRecord> iterator() {
			try {
				FileInputStream fis = new FileInputStream(cramFile);
				BufferedInputStream bis = new BufferedInputStream(fis);
				SAMIterator iterator = new SAMIterator(bis,
						referenceSequenceFile);
				iterator.setValidationStringency(validationStringency) ;
				return iterator;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public SAMRecordIterator assertSorted(SortOrder sortOrder) {
		throw new RuntimeException("Not implemented.");
	}

}
