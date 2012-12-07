package net.sf.samtools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.SAMIterator;
import net.sf.picard.reference.ReferenceSequenceFile;
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
	private SAMIterator it;
	private SeekableStream indexStream;
	private BAMIndex mIndex;
	private File mIndexFile;
	private boolean mEnableIndexCaching;
	private File mIndexStream;
	private boolean mEnableIndexMemoryMapping;

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
		this.file = bamFile ;
		this.mIndexFile = indexFile ;
		this.referenceSequenceFile = referenceSequenceFile;

		if (file == null)
			getIterator();
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

		// SeekableStream ss = null;
		// BAMIndex index = null ;
		// if (file != null)
		// try {
		// ss = new SeekableFileStream(file);
		// index = new CachingBAMFileIndex(file,
		// header.samFileHeader.getSequenceDictionary()) ;
		// } catch (FileNotFoundException e) {
		// throw new RuntimeException(
		// "Failed to open file for random access: "
		// + file.getAbsolutePath(), e);
		// }
		//
		// if (ss == null && is != null && is instanceof SeekableStream) {
		// ss = (SeekableStream) is;
		// index = new CachingBAMFileIndex(indexStream,
		// header.samFileHeader.getSequenceDictionary()) ;
		// }
		//
		// if (ss == null || index == null)
		// throw new RuntimeException(
		// "Random access is required but cannot be obtain.");
		//
		// return index;
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
		if (it != null && file == null)
			return it;
		try {
			if (file != null)
				it = new SAMIterator(new FileInputStream(file),
						referenceSequenceFile);
			else
				it = new SAMIterator(is, referenceSequenceFile);

			header = it.getCramHeader();
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
		long[] filePointers = null;

		// Hit the index to determine the chunk boundaries for the required
		// data.
		final SAMFileHeader fileHeader = getFileHeader();
		final int referenceIndex = fileHeader.getSequenceIndex(sequence);
		if (referenceIndex != -1) {
			final BAMIndex fileIndex = getIndex();
			final BAMFileSpan fileSpan = fileIndex.getSpanOverlapping(
					referenceIndex, start, end);
			filePointers = fileSpan != null ? fileSpan.toCoordinateArray()
					: null;
		}

		return null;
	}
	
	private static CloseableIterator<SAMRecord> emptyIterator  = new CloseableIterator<SAMRecord>() {

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
			return emptyIterator ; 

		SeekableStream s = null;
		if (file != null) {
			try {
				s = new SeekableFileStream(file);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else if (is instanceof SeekableStream)
			s = (SeekableStream) is;
		

		SAMIterator it = null;
		try {
			s.seek(0) ;
			it = new SAMIterator(s, referenceSequenceFile);
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}

		long offset = filePointers[0] >>> 16;
		int sliceOffset = (int) ((filePointers[0] <<48) >>> 48);
		try {
			s.seek(offset);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return it;
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
