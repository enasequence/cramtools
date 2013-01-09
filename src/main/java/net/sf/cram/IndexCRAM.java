package net.sf.cram;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.Random;

import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.CRAMFileReader;
import net.sf.samtools.CRAMIndexer;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.CloseableIterator;

public class IndexCRAM {
	private static Log log = Log.getInstance(IndexCRAM.class);

	private CountingInputStream is;
	private SAMFileHeader samFileHeader;
	private CRAMIndexer indexer;

	public IndexCRAM(InputStream is, SAMFileHeader samFileHeader, File output) {
		this.is = new CountingInputStream(is);
		this.samFileHeader = samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	public IndexCRAM(InputStream is, File output) throws IOException {
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

	public static void main(String[] args) throws IOException {
		Log.setGlobalLogLevel(LogLevel.ERROR);

		File file = new File(args[0]);
		File cramIndexFile = new File(file.getAbsolutePath() + ".brai");

		// InputStream is = new BufferedInputStream(new FileInputStream(file));
		// IndexCRAM ic = new IndexCRAM(is, cramIndexFile);
		//
		// ic.run();
		//
		// CRAMIndexer.createAndWriteIndex(cramIndexFile,
		// new File(cramIndexFile.getAbsolutePath() + ".txt"), true);
		// CRAMIndexer.createAndWriteIndex(bamIndexFile, new
		// File("c:/temp/bai.txt"), true) ;

		int position = 63693735;
		CRAMFileReader reader = new CRAMFileReader(file, cramIndexFile,
				ReferenceSequenceFileFactory.getReferenceSequenceFile(new File(
						args[1])));

		position = 62965418;
		position = 13627257;
		query(reader, position);

		int minPos = 1;
		int maxPos = 100000000;
		Random random = new Random();
		for (int i = 0; i < 10; i++)
			query(reader, random.nextInt(maxPos - minPos) + minPos);
	}

	private static void query(CRAMFileReader reader, int position) {
		CloseableIterator<SAMRecord> iterator = reader.queryAlignmentStart(
				"20", position);

		System.out.println("Query: " + position);
		SAMRecord record = null;
		int overhead = 0;
		while (iterator.hasNext()) {
			record = iterator.next();
			if (record.getAlignmentStart() >= position)
				break;
			else
				record = null;
			overhead++;
		}
		if (record == null)
			System.out.println("Nothing found.");
		else {
			System.out.println("overhead=" + overhead);
			System.out.println(record.getSAMString());
		}
		iterator.close();
	}
}
