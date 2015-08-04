package net.sf.cram;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import htsjdk.samtools.cram.build.ContainerParser;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.RuntimeEOFException;
import net.sf.cram.CramTools.LevelConverter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.sf.cram.common.Utils;

public class Refless {
	private static Log log = Log.getInstance(Refless.class);
	public static final String COMMAND = "refless";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Refless.class.getPackage().getImplementationVersion());
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

		InputStream is = null;
		try {
			is = Utils.openCramInputStream(params.cramURL, false, null);
		} catch (Exception e2) {
			log.error("Failed to open CRAM from: " + params.cramURL, e2);
			System.exit(1);
		}

		CramHeader cramHeader = CramIO.readCramHeader(is);

		if (params.printSAMHeaderOnly) {
			System.out.println(cramHeader.getSamFileHeader().getTextHeader());
			return;
		}

		Log.setGlobalLogLevel(params.logLevel);

		ContainerParser parser = new ContainerParser(cramHeader.getSamFileHeader());
		Container c = null;
		long recordCount = 0L;
		long baseCount = 0L;
		ArrayList<CramCompressionRecord> cramRecords = new ArrayList<CramCompressionRecord>();
		while (true) {
			if (params.maxContainers-- <= 0)
				break;

			c = ContainerIO.readContainer(cramHeader.getVersion(), is);
			if (c.isEOF())
				break;

			if (params.countOnly && params.requiredFlags == 0 && params.filteringFlags == 0) {
				recordCount += c.nofRecords;
				baseCount += c.bases;
				continue;
			}

			cramRecords.clear();
			try {
				parser.getRecords(c, cramRecords);
			} catch (Exception e) {
				throw new RuntimeEOFException(e);
			}

			for (CramCompressionRecord r : cramRecords) {
				if (params.requiredFlags != 0 && ((params.requiredFlags & r.flags) == 0))
					continue;
				if (params.filteringFlags != 0 && ((params.filteringFlags & r.flags) != 0))
					continue;
				if (params.countOnly) {
					recordCount++;
					baseCount += r.readLength;
					continue;
				}
			}
		}

		if (params.countOnly)
			System.out.printf("READS: %d; BASES: %d\n", recordCount, baseCount);
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-cram-file", "-I" }, description = "The path or FTP URL to the CRAM file to uncompress. Omit if standard input (pipe).")
		String cramURL;

		@Parameter(names = { "-H" }, description = "Print SAM header and quit.")
		boolean printSAMHeaderOnly = false;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--count-only", "-c" }, description = "Count number of records.")
		boolean countOnly = false;

		@Parameter(names = { "--required-flags", "-f" }, description = "Required flags. ")
		int requiredFlags = 0;

		@Parameter(names = { "--filter-flags", "-F" }, description = "Filtering flags. ")
		int filteringFlags = 0;

		@Parameter(names = { "--max-containers" }, description = "Read only specified number of containers.", hidden = true)
		long maxContainers = Long.MAX_VALUE;
	}
}
