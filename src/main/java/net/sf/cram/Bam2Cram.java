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

import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.CramLossyOptions;
import htsjdk.samtools.cram.lossy.QualityScorePreservation;
import htsjdk.samtools.util.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.ref.ReferenceSource;
import cipheronly.CipherOutputStream_256;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Bam2Cram {
	private static Log log = Log.getInstance(Bam2Cram.class);
	public static final String COMMAND = "cram";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	private static OutputStream openOutputStream(File outputFile, boolean encrypt, char[] pass)
			throws FileNotFoundException {
		OutputStream os;
		if (outputFile != null) {
			FileOutputStream fos = new FileOutputStream(outputFile);
			os = new BufferedOutputStream(fos);
		} else {
			log.warn("No output file, writint to STDOUT.");
			os = System.out;
		}

		if (encrypt) {
			CipherOutputStream_256 cos = new CipherOutputStream_256(os, pass, 128);
			os = cos.getCipherOutputStream();
		}
		return os;
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException,
			NoSuchAlgorithmException {
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

		if (params.referenceFasta == null)
			log.warn("No reference file specified, remote access over internet may be used to download public sequences. ");
		ReferenceSource referenceSource = new ReferenceSource(params.referenceFasta);

		char[] pass = null;
		if (params.encrypt) {
			if (System.console() == null)
				throw new RuntimeException("Cannot access console.");
			pass = System.console().readPassword();
		}

		SamReaderFactory f = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);

		SamReader samReader;
		if (params.bamFile == null) {
			log.warn("No input file, reading from input...");
			samReader = f.open(SamInputResource.of(System.in));
		} else
			samReader = f.open(params.bamFile);
		SAMFileHeader samFileHeader = samReader.getFileHeader().clone();

		CramLossyOptions lossyOptions = new CramLossyOptions();
		lossyOptions.setCaptureAllTags(params.captureAllTags);
		lossyOptions.setCaptureTags(params.captureTags);
		lossyOptions.setIgnoreTags(params.ignoreTags);
		lossyOptions.setPreserveReadNames(params.preserveReadNames);

		if (params.losslessQS) {
			lossyOptions.setPreservation(QualityScorePreservation.lossless());
		} else {
			if (params.qsSpec == null || params.qsSpec.length() == 0)
				lossyOptions.setPreservation(QualityScorePreservation.dropAll());
			else
				lossyOptions.setPreservation(QualityScorePreservation.lossyFromSpec(params.qsSpec));
		}
		log.info("Lossiness: " + lossyOptions);

		OutputStream os = openOutputStream(params.outputCramFile, params.encrypt, pass);

		FixBAMFileHeader fixBAMFileHeader = new FixBAMFileHeader(referenceSource);
		fixBAMFileHeader.setConfirmMD5(params.confirmMD5);
		fixBAMFileHeader.setInjectURI(params.injectURI);
		fixBAMFileHeader.setIgnoreMD5Mismatch(params.ignoreMD5Mismatch);
		try {
			fixBAMFileHeader.fixSequences(samFileHeader.getSequenceDictionary().getSequences());
		} catch (FixBAMFileHeader.MD5MismatchError e) {
			log.error(e.getMessage());
			System.exit(1);
		}
		fixBAMFileHeader.addCramtoolsPG(samFileHeader);

		CRAMContainerStreamWriter w = new CRAMContainerStreamWriter(os, null, referenceSource, samFileHeader,
				params.bamFile == null ? null : params.bamFile.getName(), lossyOptions);
		w.writeHeader(samFileHeader);

		if (params.queries == null || params.queries.isEmpty()) {
			SAMRecordIterator iterator = samReader.iterator();
			while (iterator.hasNext()) {
				if (params.outputCramFile == null && System.out.checkError())
					return;

				SAMRecord samRecord = iterator.next();
				w.writeAlignment(samRecord);
			}
			iterator.close();
		} else {
			List<AlignmentSliceQuery> queries = new ArrayList<AlignmentSliceQuery>();
			for (String string : params.queries) {
				try {
					queries.add(new AlignmentSliceQuery(string));
				} catch (Exception e) {
					log.error("Failed to parse query: " + string);
					System.exit(1);
				}
			}

			for (AlignmentSliceQuery query : queries) {
				SAMRecordIterator iterator = samReader.query(query.sequence, query.start, query.end, false);
				while (iterator.hasNext()) {
					if (params.outputCramFile == null && System.out.checkError())
						return;

					SAMRecord samRecord = iterator.next();
					w.writeAlignment(samRecord);
				}
				iterator.close();
			}
		}

		samReader.close();
		w.finish(true);
		os.close();
	}

	@Parameters(commandDescription = "BAM to CRAM converter. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-bam-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM file to be converted to CRAM. Omit if standard input (pipe).")
		File bamFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFasta;

		@Parameter(names = { "--output-cram-file", "-O" }, converter = FileConverter.class, description = "The path for the output CRAM file. Omit if standard output (pipe).")
		File outputCramFile = null;

		@Parameter(names = { "--max-records" }, description = "Stop after compressing this many records. ")
		long maxRecords = Long.MAX_VALUE;

		@Parameter
		List<String> queries;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--preserve-read-names", "-n" }, description = "Preserve all read names.")
		boolean preserveReadNames = false;

		@Parameter(names = { "--lossless-quality-score", "-Q" }, description = "Preserve all quality scores. Overwrites '--lossless-quality-score'.")
		boolean losslessQS = false;

		@Parameter(names = { "--lossy-quality-score-spec", "-L" }, description = "A string specifying what quality scores should be preserved.")
		String qsSpec = "";

		@Parameter(names = { "--encrypt" }, description = "Encrypt the CRAM file.")
		boolean encrypt = false;

		@Parameter(names = { "--ignore-tags" }, description = "Ignore the tags listed, for example 'OQ:XA:XB'")
		String ignoreTags = "";

		@Parameter(names = { "--capture-tags" }, description = "Capture the tags listed, for example 'OQ:XA:XB'")
		String captureTags = "";

		@Parameter(names = { "--capture-all-tags" }, description = "Capture all tags.")
		boolean captureAllTags = false;

		@Parameter(names = { "--input-is-sam" }, description = "Input is in SAM format.")
		boolean inputIsSam = false;

		@Parameter(names = { "--inject-sq-uri" }, description = "Inject or change the @SQ:UR header fields to point to ENA reference service. ")
		public boolean injectURI = false;

		@Parameter(names = { "--ignore-md5-mismatch" }, description = "Fail on MD5 mismatch if true, or correct (overwrite) the checksums and continue if false.")
		public boolean ignoreMD5Mismatch = false;

		@Parameter(names = { "--confirm-md5" }, description = "Confirm MD5 checksums of the reference sequences.", hidden = true, arity = 1)
		public boolean confirmMD5 = true;
	}
}
