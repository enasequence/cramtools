/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.ProgressLoggerInterface;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.zip.GZIPOutputStream;

public class FastqSAMFileWriter implements SAMFileWriter {
	private PrintStream s1, s2;
	private SAMFileHeader header;
	private FileOutputStream fos1;
	private FileOutputStream fos2;

	public FastqSAMFileWriter(PrintStream s1, PrintStream s2, SAMFileHeader header) {
		this.s1 = s1;
		this.s2 = s2;
		this.header = header;
	}

	public FastqSAMFileWriter(String baseFileName, boolean gziped, SAMFileHeader header) throws IOException {
		File f1 = new File(baseFileName + "_1.fq" + (gziped ? ".gz" : ""));
		File f2 = new File(baseFileName + "_2.fq" + (gziped ? ".gz" : ""));

		fos1 = new FileOutputStream(f1);
		fos2 = new FileOutputStream(f2);

		if (gziped) {
			this.s1 = new PrintStream(new BufferedOutputStream(new GZIPOutputStream(fos1)));
			this.s2 = new PrintStream(new BufferedOutputStream(new GZIPOutputStream(fos2)));
		} else {
			this.s1 = new PrintStream(new BufferedOutputStream(fos1));
			this.s2 = new PrintStream(new BufferedOutputStream(fos2));
		}

		this.header = header;
	}

	@Override
	public void addAlignment(SAMRecord alignment) {
		PrintStream ps = s1;
		if (s2 != null && alignment.getReadPairedFlag() && alignment.getFirstOfPairFlag())
			ps = s2;

		printFastq(ps, alignment);
	}

	private static void printFastq(PrintStream ps, SAMRecord record) {
		ps.print('@');
		ps.println(record.getReadName());
		ps.println(record.getReadString());
		ps.println('+');
		ps.println(record.getBaseQualityString());
	}

	@Override
	public SAMFileHeader getFileHeader() {
		return header;
	}

	@Override
	public void setProgressLogger(ProgressLoggerInterface progressLoggerInterface) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void close() {
		if (s1 != null)
			s1.flush();
		if (s2 != null)
			s2.flush();

		if (fos1 != null)
			try {
				fos1.close();
			} catch (IOException e) {
			}
		if (fos2 != null)
			try {
				fos2.close();
			} catch (IOException e) {
			}
	}

}
