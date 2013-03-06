package net.sf.cram.index;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.CountingInputStream;
import net.sf.cram.Index;
import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;

class CraiIndexer {
	private static Log log = Log.getInstance(CraiIndexer.class);

	private CountingInputStream is;
	private SAMFileHeader samFileHeader;
	private Index index;

	public CraiIndexer(InputStream is, File output)
			throws FileNotFoundException, IOException {
		this.is = new CountingInputStream(is);
		CramHeader cramHeader = ReadWrite.readCramHeader(this.is);
		samFileHeader = cramHeader.samFileHeader ;
		
		index = new Index(new GZIPOutputStream(new BufferedOutputStream(
				new FileOutputStream(output))));
		
	}

	private void nextContainer() throws IOException {
		long offset = is.getCount();
		Container c = ReadWrite.readContainer(samFileHeader, is);
		c.offset = offset;
		index.addContainer(c);
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
		index.close();
	}
}