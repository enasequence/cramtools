package net.sf.cram.index;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import net.sf.cram.CountingInputStream;
import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.samtools.CRAMIndexer;
import net.sf.samtools.SAMFileHeader;

class BaiIndexer {
	private static Log log = Log.getInstance(BaiIndexer.class);

	public CountingInputStream is;
	public SAMFileHeader samFileHeader;
	public CRAMIndexer indexer;

	public BaiIndexer(InputStream is, SAMFileHeader samFileHeader,
			File output) {
		this.is = new CountingInputStream(is);
		this.samFileHeader = samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	public BaiIndexer(InputStream is, File output) throws IOException {
		this.is = new CountingInputStream(is);
		CramHeader cramHeader = ReadWrite.readCramHeader(this.is);
		samFileHeader = cramHeader.samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	private void nextContainer() throws IOException {
		long offset = is.getCount();
		Container c = ReadWrite.readContainer(samFileHeader, is);
		c.offset = offset;

		int i = 0;
		for (Slice slice : c.slices) {
			slice.containerOffset = offset;
			slice.index = i++;
			indexer.processAlignment(slice);
		}

		log.info("INDEXED: " + c.toString());
	}

	private void index() throws IOException {
		while (true) {
			try {
				nextContainer();
			} catch (EOFException e) {
				break;
			}
		}
	}

	public void run() throws IOException {
		index();
		indexer.finish();
	}
}