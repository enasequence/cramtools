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
package net.sf.cram.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.CRAMFileReader;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.Log;
import net.sf.cram.Bam2Cram;
import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.ref.ReferenceSource;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class CramIndexer {
	private static Log log = Log.getInstance(CramIndexer.class);
	public static final String COMMAND = "index";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		boolean cramOrNot = false;
		try {
			CramIO.readCramHeader(new FileInputStream(params.inputFile));
			cramOrNot = true ;
		} catch (Exception e) {
			cramOrNot = false;
		} finally {
		}
		if (cramOrNot) {
			if (params.bai) {

				File cramIndexFile = new File(params.inputFile.getAbsolutePath() + ".bai");

				create_BAI_forCramFile(params.inputFile, cramIndexFile);

				if (params.test) {
					if (params.referenceFastaFile == null) {
						System.out.println("A reference fasta file is required.");
						System.exit(1);
					}

					randomTestCramFileWithBaiIndex(params.inputFile, cramIndexFile, params.referenceFastaFile,
							params.testMinPos, params.testMaxPos, params.testCount, params.testSequenceName);
				}
			} else {
				File cramIndexFile = new File(params.inputFile.getAbsolutePath() + ".crai");

				create_CRAI_forCramFile(params.inputFile, cramIndexFile);

			}
		} else {
			if (!params.bai)
				throw new RuntimeException("CRAM index is not compatible with BAM files.");

			SAMFileReader reader = new SAMFileReader(params.inputFile);
			if (!reader.isBinary()) {
				reader.close();
				throw new SAMException("Input file must be bam file, not sam file.");
			}

			if (!reader.getFileHeader().getSortOrder().equals(SAMFileHeader.SortOrder.coordinate)) {
				reader.close();
				throw new SAMException("Input bam file must be sorted by coordinates");
			}

			File indexFile = new File(params.inputFile.getAbsolutePath() + ".bai");
			BAMIndexer indexer = new BAMIndexer(indexFile, reader.getFileHeader());
			for (SAMRecord record : reader) {
				indexer.processAlignment(record);
			}
			indexer.finish();
			reader.close();
		}

	}

	@Parameters(commandDescription = "BAM/CRAM indexer. ")
	public static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM or CRAM file to be indexed. Omit if standard input (pipe).")
		File inputFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, hidden = true, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFastaFile;

		@Parameter(names = { "--bam-style-index" }, description = "Choose between BAM index (bai) and CRAM index (crai). ")
		boolean bai = false;

		@Parameter(names = { "--help", "-h" }, description = "Print help and exit.")
		boolean help = false;

		@Parameter(names = { "--test" }, hidden = true, description = "Random test of the built index.")
		boolean test = false;

		@Parameter(names = { "--test-min-pos" }, hidden = true, description = "Minimum alignment start for randomt test.")
		int testMinPos = 1;

		@Parameter(names = { "--test-max-pos" }, hidden = true, description = "Maximum alignment start for randomt test.")
		int testMaxPos = 100000000;

		@Parameter(names = { "--test-count" }, hidden = true, description = "Run random test this many times.")
		int testCount = 100;

		@Parameter(names = { "--test-sequence-name" }, hidden = true, description = "Run random test for this sequence. ")
		String testSequenceName = null;
	}

	public static void create_BAI_forCramFile(File cramFile, File cramIndexFile) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		BaiIndexer ic = new BaiIndexer(is, cramIndexFile);

		ic.run();
	}

	public static void create_CRAI_forCramFile(File cramFile, File cramIndexFile) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		CraiIndexer ic = new CraiIndexer(is, cramIndexFile);

		ic.run();
	}

	/**
	 * @param cramFile
	 * @param cramIndexFile
	 * @param refFile
	 * @param posMin
	 * @param posMax
	 * @param repeat
	 * @return the overhead, the number of records skipped before reached the
	 *         query or -1 if nothing was found.
	 */
	private static int randomTestCramFileWithBaiIndex(File cramFile, File cramIndexFile, File refFile, int posMin,
			int posMax, int repeat, String sequenceName) {
		CRAMFileReader reader = new CRAMFileReader(cramFile, cramIndexFile, new ReferenceSource(refFile));

		int overhead = 0;

		Random random = new Random();
		for (int i = 0; i < repeat; i++) {
			int result = 0;
			int pos = random.nextInt(posMax - posMin) + posMin;
			try {
				result = query(reader, pos, sequenceName);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(String.format("Query failed at %d.", pos));
			}
			if (result > -1)
				overhead += repeat;
		}
		return overhead;
	}

	private static int query(CRAMFileReader reader, int position, String sequenceName) {
		long timeStart = System.nanoTime();

		CloseableIterator<SAMRecord> iterator = reader.queryAlignmentStart(sequenceName, position);

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
		iterator.close();

		long timeStop = System.nanoTime();
		if (record == null)
			log.info(String.format("Query not found: position=%d, time=%dms.", position,
					(timeStop - timeStart) / 1000000));
		else
			log.info(String.format("Query found: position=%d, overhead=%d, time=%dms.", position, overhead,
					(timeStop - timeStart) / 1000000));

		return overhead;
	}
}
