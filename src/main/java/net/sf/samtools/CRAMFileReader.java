package net.sf.samtools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.SAMIterator;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.CloseableIterator;

public class CRAMFileReader extends SAMFileReader.ReaderImplementation {
	private File file;
	private ReferenceSequenceFile referenceSequenceFile;
	private CramHeader header;

	public CRAMFileReader(File file, ReferenceSequenceFile referenceSequenceFile) {
		super();
		this.file = file;
		this.referenceSequenceFile = referenceSequenceFile;
	}

	private void readHeader() throws FileNotFoundException, IOException {
		header = ReadWrite.readCramHeader(new FileInputStream(file));
	}

	@Override
	void enableFileSource(SAMFileReader reader, boolean enabled) {
		// TODO Auto-generated method stub

	}

	@Override
	void enableIndexCaching(boolean enabled) {
		// TODO Auto-generated method stub

	}

	@Override
	void enableIndexMemoryMapping(boolean enabled) {
		// TODO Auto-generated method stub

	}

	@Override
	void enableCrcChecking(boolean enabled) {
		// TODO Auto-generated method stub

	}

	@Override
	void setSAMRecordFactory(SAMRecordFactory factory) {
		// TODO Auto-generated method stub

	}

	@Override
	boolean hasIndex() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	BAMIndex getIndex() {
		// TODO Auto-generated method stub
		return null;
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
	CloseableIterator<SAMRecord> getIterator() {
		SAMIterator it;
		try {
			it = new SAMIterator(new FileInputStream(file),
					referenceSequenceFile);
			return it;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	CloseableIterator<SAMRecord> getIterator(SAMFileSpan fileSpan) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	SAMFileSpan getFilePointerSpanningReads() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	CloseableIterator<SAMRecord> query(String sequence, int start, int end,
			boolean contained) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	CloseableIterator<SAMRecord> queryAlignmentStart(String sequence, int start) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIterator<SAMRecord> queryUnmapped() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub

	}

	@Override
	void setValidationStringency(ValidationStringency validationStringency) {
		// TODO Auto-generated method stub

	}

	@Override
	ValidationStringency getValidationStringency() {
		// TODO Auto-generated method stub
		return null;
	}
}
