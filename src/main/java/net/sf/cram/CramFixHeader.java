package net.sf.cram;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.Log;
import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.FixBAMFileHeader.MD5MismatchError;
import net.sf.cram.common.Utils;
import net.sf.cram.ref.ReferenceSource;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class CramFixHeader {
	private static Log log = Log.getInstance(CramFixHeader.class);
	public static final String COMMAND = "fixheader";

	public static void main(String[] args) throws IOException, MD5MismatchError {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.setProgramName(COMMAND);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameters, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			Utils.printUsage(jc);
			System.exit(0);
		}

		Log.setGlobalLogLevel(params.logLevel);

		if (params.cramFile == null) {
			log.error("CRAM file is required. ");
			System.exit(1);
		}

		if (params.reference == null && params.confirmMD5) {
			log.error("Reference file is required to confirm MD5s. ");
			System.exit(1);
		}

		try {
			if (!checkURIPattenIsSensible(params.sequenceUrlPattern)) {
				log.error("URI pattern is not valid.");
				System.exit(1);
			}
		} catch (URISyntaxException e) {
			log.error(e.getMessage());
			System.exit(1);
		}

		ReferenceSource referenceSource = params.reference == null ? null : new ReferenceSource(params.reference);

		FileInputStream fis = new FileInputStream(params.cramFile);
		CramHeader cramHeader = CramIO.readCramHeader(fis);

		FixBAMFileHeader fixer = new FixBAMFileHeader(referenceSource);
		fixer.setIgnoreMD5Mismatch(true);
		fixer.fixSequences(cramHeader.getSamFileHeader().getSequenceDictionary().getSequences());
		fixer.addCramtoolsPG(cramHeader.getSamFileHeader());

		CramHeader newHeader = cramHeader.clone();

		if (!CramIO.replaceCramHeader(params.cramFile, newHeader)) {
			log.error("Failed to replace the header.");
			System.exit(1);
		}

	}

	private static boolean checkURIPattenIsSensible(String pattern) throws URISyntaxException {
		String uri = String.format(pattern, "d41d8cd98f00b204e9800998ecf8427e");
		URI u = new URI(uri);
		// the uri has been parsed and contains the md5:
		return (u.toASCIIString().contains(uri));
	}

	@Parameters(commandDescription = "A tool to fix CRAM header without re-writing the whole file.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-cram-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM file.")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example). ")
		File reference;

		@Parameter(names = { "-h", "--help" })
		boolean help = false;

		@Parameter(names = { "--confirm-md5" }, description = "Calculate MD5 for sequences mentioned in the header. Requires --reference-fasta-file option.")
		boolean confirmMD5 = false;

		@Parameter(names = { "--inject-uri" }, description = "Inject URI for all reference sequences in the header.")
		boolean injectURI = false;

		@Parameter(names = { "--uri-pattern" }, description = "String formatting pattern for sequence URI to be injected.")
		String sequenceUrlPattern = "http://www.ebi.ac.uk/ena/cram/md5/%s";

	}
}
