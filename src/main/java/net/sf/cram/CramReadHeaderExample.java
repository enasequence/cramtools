package net.sf.cram;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMSequenceRecord;

public class CramReadHeaderExample {
	private static final String URI_PATTERN = "http://www.ebi.ac.uk/ena/cram/md5/%s";

	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);
		SAMFileReader reader = new SAMFileReader(file);
		for (SAMSequenceRecord sequenceRecord : reader.getFileHeader()
				.getSequenceDictionary().getSequences()) {
			String md5 = sequenceRecord.getAttribute(SAMSequenceRecord.MD5_TAG);
			URL url = new URL(String.format(URI_PATTERN, md5));
			InputStream is = null;
			try {
				is = url.openStream();
				System.out.printf("Found: %s\t%s\n",
						sequenceRecord.getSequenceName(), md5);
				is.close();
			} catch (IOException e) {
				System.out.printf("Not found: %s\t%s\n",
						sequenceRecord.getSequenceName(), md5);
			}
		}
		reader.close();
	}
}
