package net.sf.cram.index;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.build.CramIO;
import net.sf.cram.io.CountingInputStream;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;

class CraiIndexer {
	private static Log log = Log.getInstance(CraiIndexer.class);

	private CountingInputStream is;
	private SAMFileHeader samFileHeader;
	private CramIndex index;

	public CraiIndexer(InputStream is, File output)
			throws FileNotFoundException, IOException {
		this.is = new CountingInputStream(is);
		CramHeader cramHeader = CramIO.readCramHeader(this.is);
		samFileHeader = cramHeader.samFileHeader;

		index = new CramIndex(new GZIPOutputStream(new BufferedOutputStream(
				new FileOutputStream(output))));

	}

	private boolean nextContainer() throws IOException {
		long offset = is.getCount();
		Container c = CramIO.readContainer(is);
		if (c == null)
			return false;
		c.offset = offset;
		index.addContainer(c);
		log.info("INDEXED: " + c.toString());
		return true;
	}

	private void index() throws IOException {
		while (true) {
			if (!nextContainer())
				break;
		}
	}

	public void run() throws IOException {
		index();
		index.close();
	}
}