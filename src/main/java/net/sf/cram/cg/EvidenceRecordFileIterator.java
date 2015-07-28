package net.sf.cram.cg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


import htsjdk.samtools.util.CloseableIterator;
import org.apache.tools.bzip2.CBZip2InputStream;

class EvidenceRecordFileIterator implements CloseableIterator<EvidenceRecord> {
	private BufferedReader reader;
	private EvidenceRecord cachedRecord;
	String assembly_ID;
	String chromosome;
	String sample;
	String generatedAt;
	String line;
	long counter = 0;

	EvidenceRecordFileIterator(File file) throws FileNotFoundException, IOException {
		InputStream is = new FileInputStream(file);
		// have to skip first two bytes due to some apache bzip2 peculiarity:
		is.read();
		is.read();
		reader = new BufferedReader(new InputStreamReader(new CBZip2InputStream(is)));
		readHeader();
		readNext();
	};

	private void readHeader() {
		try {
			while ((line = reader.readLine()) != null) {
				if (line.startsWith(">"))
					return;
				if (line.length() == 0)
					continue;
				if (line.startsWith("#")) {
					String[] words = line.split("\\t");
					if (words.length != 2)
						throw new RuntimeException("File header format error.");
					if ("#ASSEMBLY_ID".equals(words[0])) {
						assembly_ID = words[1];
					} else if ("#CHROMOSOME".equals(words[0])) {
						chromosome = words[1];
					} else if ("#SAMPLE".equals(words[0])) {
						sample = words[1];
					} else if ("#GENERATED_AT".equals(words[0])) {
						generatedAt = words[1];
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void readNext() {
		cachedRecord = null;
		try {
			while ((line = reader.readLine()) != null) {
				counter++;
				cachedRecord = EvidenceRecord.fromString(line);
				break;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return cachedRecord != null;
	}

	EvidenceRecord peek() {
		return cachedRecord;
	}

	@Override
	public EvidenceRecord next() {
		EvidenceRecord next = cachedRecord;
		readNext();
		return next;
	}

	@Override
	public void remove() {
		throw new RuntimeException("Remove not supported.");
	}

	@Override
	public void close() {
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}

}
