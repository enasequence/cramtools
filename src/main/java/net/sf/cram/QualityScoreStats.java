package net.sf.cram;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.build.ContainerParser;
import net.sf.cram.build.CramIO;
import net.sf.cram.build.CramNormalizer;
import net.sf.cram.lossy.Binning;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.CramRecord;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class QualityScoreStats {
	private static Log log = Log.getInstance(QualityScoreStats.class);
	public static final String COMMAND = "qstat";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + QualityScoreStats.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws Exception {
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

		dist(params.inputFile, (byte) (0xFF & params.defaultQualityScore));
	}

	private static void dist(File file, byte defaultQualityScore) throws IllegalArgumentException, IOException,
			IllegalAccessException {
		InputStream is = new FileInputStream(file);
		CramHeader header = CramIO.readCramHeader(is);
		Container c = null;
		ContainerParser parser = new ContainerParser(header.samFileHeader);
		ArrayList<CramRecord> records = new ArrayList<CramRecord>(10000);

		long[] freq = new long[255];
		while ((c = CramIO.readContainer(is)) != null) {
			parser.getRecords(c, records);

			for (CramRecord record : records) {
				byte[] scores = CramNormalizer.restoreQualityScores(defaultQualityScore, record);
				for (byte b : scores)
					freq[b & 0xFF]++;
			}
			records.clear();
		}
		print(freq, defaultQualityScore, System.out);
	}

	private static void print(long[] array, byte defaultQualityScore, PrintStream ps) {
		Pair[] pairs = new Pair[array.length];
		long max = 0;
		for (int i = 0; i < array.length; i++) {
			pairs[i] = new Pair(array[i], (byte) i);
			max = max < array[i] ? array[i] : max;
		}
		Arrays.sort(pairs, new Comparator<Pair>() {

			@Override
			public int compare(Pair o1, Pair o2) {
				return (int) (o2.count - o1.count);
			}
		});

		int maxLen = String.valueOf(max).length();
		String format = "%1$" + maxLen + "s [%2$c] %3$s %4$2d\n";
		for (Pair pair : pairs) {
			if (pair.count > 0) {
				StringBuilder qualifier = new StringBuilder();
				if (pair.value == defaultQualityScore) {
					qualifier.append("*");
				} else
					qualifier.append(" ");
				if (Arrays.binarySearch(Binning.Illumina_binning_matrix, pair.value) > -1)
					qualifier.append(" 8");
				else
					qualifier.append(" 40");

				boolean isDefault = pair.value == defaultQualityScore;
				boolean binned = Arrays.binarySearch(Binning.Illumina_binning_matrix, pair.value) > -1;

				ps.printf(format, String.valueOf(pair.count), (char) (pair.value + 33), isDefault ? "*" : " ",
						binned ? 8 : 40);
			}
		}
	};

	private static class Pair {
		long count;
		byte value;

		public Pair(long count, byte value) {
			this.count = count;
			this.value = value;
		}
	}

	@Parameters(commandDescription = "Quality score statistics.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM or BAM file.")
		File inputFile;

		@Parameter(names = { "--default-quality-score" }, description = "Use this value as a default or missing quality score. Lowest is 0, which corresponds to '!' in fastq.")
		int defaultQualityScore = 30;
	}
}
