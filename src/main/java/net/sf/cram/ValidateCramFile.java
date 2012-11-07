package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.ReadWrite.CramHeader;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.sam.SamFileValidator;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.picard.util.ProgressLogger;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMValidationError;
import net.sf.samtools.SAMValidationError.Type;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class ValidateCramFile {
	private static Log log = Log.getInstance(ValidateCramFile.class);

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ ValidateCramFile.class.getPackage()
						.getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out
					.println("Failed to parse parameteres, detailed message below: ");
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

		Log.setGlobalLogLevel(LogLevel.INFO);

		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(params.reference);

		FileInputStream fis = new FileInputStream(params.cramFile);
		BufferedInputStream bis = new BufferedInputStream(fis);

		CramFileIterator iterator = new CramFileIterator(bis,
				referenceSequenceFile);

		CramHeader cramHeader = iterator.getCramHeader();

		ProgressLogger progress = new ProgressLogger(log, 100000,
				"Validated Read");
		SamFileValidator v = new SamFileValidator(new PrintWriter(System.out),
				1);
		List<SAMValidationError.Type> errors = new ArrayList<>();
		errors.add(Type.MATE_NOT_FOUND);
		errors.add(Type.MISSING_TAG_NM);
		v.setErrorsToIgnore(errors);
		v.init(referenceSequenceFile, cramHeader.samFileHeader);
		while (iterator.hasNext()) {
			SAMRecord s = iterator.next();
			v.validateRecord(progress, s, cramHeader.samFileHeader);
		}
		log.info("Elapsed seconds: " + progress.getElapsedSeconds());
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {
		@Parameter(names = { "--input-cram-file" }, converter = FileConverter.class, description = "The path to the CRAM file to uncompress. Omit if standard input (pipe).")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;
	}

}
