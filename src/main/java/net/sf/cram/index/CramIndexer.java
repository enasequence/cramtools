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

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.FileFormat;
import htsjdk.samtools.IndexFormat;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.util.Log;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;

import net.sf.cram.CramTools.LevelConverter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class CramIndexer {
	private static Log log = Log.getInstance(CramIndexer.class);
	public static final String COMMAND = "index";
	private static int BUFFER_SIZE = 100 * 1024;

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + CramIndexer.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
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

		if (!EnumSet.of(IndexFormat.BAI, IndexFormat.CRAI).contains(params.indexFormat)) {
			failWithError(String.format("Unexpected index format (%s).", params.indexFormat.name()));
		}

		BufferedInputStream is = null;
		String source = null;
		if (params.inputFile == null)
			is = new BufferedInputStream(System.in);
		else {
			is = new BufferedInputStream(new FileInputStream(params.inputFile), BUFFER_SIZE);
			source = params.inputFile.getName();
		}

		FileFormat format = FileFormat.detect(is, source);
		if (!format.testHeader(is)) {
			failWithError("On a second thought, this is not the format it seems: " + format.name());
		}

		if (!isValidInputCombination(format, params.indexFormat)) {
			failWithError(String.format("Unexpected combination of file format (%s) and index format (%s).",
					format.name(), params.indexFormat.name()));
		}

		// open raw {@link: OutputStream} for index file:
		OutputStream indexOutputStream = null;
		if (params.indexFile != null) {
			indexOutputStream = new FileOutputStream(params.indexFile);
		} else {
			if (params.inputFile == null)
				indexOutputStream = System.out;
			else
				indexOutputStream = new FileOutputStream(
						params.indexFormat.getDefaultIndexFileNameForDataFile(params.inputFile));
		}

		switch (format) {
		case CRAM:
			switch (params.indexFormat) {
			case BAI:
				new BaiIndexer(is, indexOutputStream).run();
				break;
			case CRAI:
				new CraiIndexer(is, indexOutputStream).run();
				break;

			default:
				failWithError("Expecting CRAI or BAI for CRAM input.");
			}
			break;
		case BAM:
			if (params.indexFormat != IndexFormat.BAI)
				failWithError("CRAM index is not compatible with BAM files.");

			SamReader reader = SamReaderFactory.make()
					.setOption(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS, true).open(SamInputResource.of(is));

			if (!reader.getFileHeader().getSortOrder().equals(SAMFileHeader.SortOrder.coordinate)) {
				reader.close();
				failWithError("Input bam file must be sorted by coordinates");
			}

			BAMIndexer indexer = new BAMIndexer(indexOutputStream, reader.getFileHeader());

			for (SAMRecord record : reader) {
				indexer.processAlignment(record);
			}
			indexer.finish();
			reader.close();
			break;
		default:
			failWithError("Failed to recognize input file format.");
		}
	}

	private static void failWithError(String message) {
		log.error(message);
		System.exit(1);
	}

	private static boolean isValidInputCombination(FileFormat inputFileFormat, IndexFormat indexFileFormat) {
		switch (inputFileFormat) {
		case CRAM:
			return EnumSet.of(IndexFormat.BAI, IndexFormat.CRAI).contains(indexFileFormat);
		case BAM:
			return IndexFormat.BAI == indexFileFormat;
		default:
			return false;
		}
	}

	public static class IndexTypeConverter implements IStringConverter<IndexFormat> {

		@Override
		public IndexFormat convert(String indexNameString) {
			return IndexFormat.fromString(indexNameString);
		}

	};

	@Parameters(commandDescription = "BAM/CRAM indexer. ")
	public static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM or CRAM file to be indexed. Omit if standard input (pipe).")
		File inputFile;

		@Parameter(names = { "--index-file", "-O" }, converter = FileConverter.class, description = "Write index to this file.")
		File indexFile;

		@Parameter(names = { "--index-format" }, converter = IndexTypeConverter.class, description = "Choose between BAM index (bai) and CRAM index (crai). ")
		IndexFormat indexFormat = IndexFormat.CRAI;

		@Parameter(names = { "--help", "-h" }, description = "Print help and exit.")
		boolean help = false;
	}
}
