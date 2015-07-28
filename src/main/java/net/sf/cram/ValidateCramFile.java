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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import htsjdk.samtools.CRAMIterator;
import htsjdk.samtools.SAMValidationError;
import htsjdk.samtools.SamFileValidator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.ProgressLogger;
import net.sf.cram.ref.ReferenceSource;

public class ValidateCramFile {
	private static Log log = Log.getInstance(ValidateCramFile.class);

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + ValidateCramFile.class.getPackage().getImplementationVersion());
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

		if (params.reference == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		if (params.cramFile == null) {
			System.out.println("A CRAM input file is required. ");
			System.exit(1);
		}

		Log.setGlobalLogLevel(Log.LogLevel.INFO);

		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(params.reference);

		FileInputStream fis = new FileInputStream(params.cramFile);
		BufferedInputStream bis = new BufferedInputStream(fis);

		CRAMIterator iterator = new CRAMIterator(bis,new ReferenceSource(params.reference));
		CramHeader cramHeader = iterator.getCramHeader();

		iterator.close();

		ProgressLogger progress = new ProgressLogger(log, 100000, "Validated Read");
		SamFileValidator v = new SamFileValidator(new PrintWriter(System.out), 1);
		final SamReader reader = SamReaderFactory.make().referenceSequence(params.reference).open(params.cramFile);
		List<SAMValidationError.Type> errors = new ArrayList<SAMValidationError.Type>();
		errors.add(SAMValidationError.Type.MATE_NOT_FOUND);
		// errors.add(Type.MISSING_TAG_NM);
		v.setErrorsToIgnore(errors);
		v.validateSamFileSummary(reader, ReferenceSequenceFileFactory.getReferenceSequenceFile(params.reference));
		log.info("Elapsed seconds: " + progress.getElapsedSeconds());
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {
		@Parameter(names = { "--input-cram-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM file to uncompress. Omit if standard input (pipe).")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;
	}

}
