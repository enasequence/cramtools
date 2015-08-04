package net.sf.cram;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.Log;
import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.common.Utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class RefRepo {
	private static Log log = Log.getInstance(RefRepo.class);
	public static final String COMMAND = "repo";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + RefRepo.class.getPackage().getImplementationVersion());
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

		if (params.repoFile == null) {
			System.out.println("Expecting repo file.");
			printUsage(jc);
			System.err.println(1);
		}

		if (params.repoFile.exists())
			readRepoFile(params.repoFile);

		Set<String> files = listAllFiles(params.pathsToAdd);

		for (String path : files) {
			if (byFile.containsKey(path) && !params.refreshPathCollisions)
				continue;

			for (Entry e : readFile(new File(path)))
				put(e);
		}

		for (Entry e : readFiles(files, params.parallel)) {
			put(e);
		}

		save(params.repoFile);
	}

	private static void save(File file) throws IOException {
		FileWriter w = new FileWriter(file, false);
		for (Entry e : map.values()) {
			w.write(e.toString());
			w.write('\n');
		}
		w.close();
	}

	private static List<Entry> readFiles(Collection<String> paths, int threads) {
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		List<Entry> list;
		try {
			Set<Future<List<Entry>>> futures = new HashSet<Future<List<Entry>>>();

			for (String path : paths) {
				FileJob job = new FileJob(new File(path));
				log.info("Submitting job: ", path);
				Future<List<Entry>> future = pool.submit(job);
				futures.add(future);
			}

			list = new ArrayList<RefRepo.Entry>();
			for (Future<List<Entry>> f : futures) {
				try {
					list.addAll(f.get());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		} finally {
			pool.shutdownNow();
		}

		return list;
	}

	private static List<Entry> readFile(File file) {
		List<Entry> entries = new ArrayList<RefRepo.Entry>();
		return entries;
	}

	private static class FileJob implements Callable<List<Entry>> {
		private File file;

		public FileJob(File file) {
			this.file = file;
		}

		@Override
		public List<Entry> call() throws Exception {
			List<Entry> list = new ArrayList<RefRepo.Entry>();

			ReferenceSequenceFile rsFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(file);

			ReferenceSequence sequence = null;
			while ((sequence = rsFile.nextSequence()) != null) {
				sequence.getBases();

				Entry e = new Entry();
				e.md5 = Utils.calculateMD5String(sequence.getBases());
				e.file = "file://" + file.getAbsolutePath();
				e.name = sequence.getName();
				e.length = sequence.length();
				log.info(String.format("New entry: %s", e.toString()));
				list.add(e);
			}
			return list;
		}
	};

	private static Set<String> listAllFiles(Collection<String> paths) {
		Set<String> set = new TreeSet<String>();
		for (String path : paths) {
			File file = new File(path);
			if (!file.exists()) {
				log.warn("File or directory does not exist: " + path);
				continue;
			}

			if (!file.canRead()) {
				log.warn("Cannot read file or directory: " + path);
				continue;
			}

			if (file.isDirectory()) {
				List<String> subPaths = new ArrayList<String>();
				for (File f : file.listFiles())
					subPaths.add(f.getAbsolutePath());
				set.addAll(listAllFiles(subPaths));
				continue;
			}

			if (!file.isFile()) {
				log.warn("Neither file nor directory: " + path);
				continue;
			}

			if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")) {
				set.add(file.getAbsolutePath());
			}
		}
		return set;
	}

	private static Map<String, Entry> byFile = new TreeMap<String, RefRepo.Entry>();
	private static Map<String, Entry> byFileAndName = new TreeMap<String, RefRepo.Entry>();
	private static Map<String, Entry> map = new HashMap<String, RefRepo.Entry>();

	private static Entry parse(String line) {
		Pattern pattern = Pattern.compile("^@SQ\tSN:(\\w+)\tLN:(\\d+)\tUR:(\\w+)\tM5:([a-z0-9]+)$");
		Matcher m = pattern.matcher(line);
		if (!m.matches())
			throw new RuntimeException("Improper format: " + line);

		Entry e = new Entry();
		e.name = m.group(1);
		e.length = Integer.valueOf(m.group(2));
		e.file = m.group(3);
		e.md5 = m.group(4);
		return e;
	}

	private static void readRepoFile(File file) throws FileNotFoundException {
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			Entry e = parse(line);
			put(e);
		}
	}

	private static void put(Entry e) {
		map.put(e.md5, e);
		byFile.put(e.file, e);
		byFileAndName.put(String.format("%s:%s", e.file, e.name), e);
	}

	private static class Entry {
		String md5;
		int length;
		String file;
		String name;

		@Override
		public String toString() {
			return String.format("@SQ\tSN:%s\tLN:%d\tUR:%s\tM5:%s", name, length, file, md5);
		}
	}

	@Parameters(commandDescription = "Register local reference fasta files in the repo file.")
	static class Params {

		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--repo-file", "-R" }, converter = FileConverter.class, description = "The path to the repository description file.")
		File repoFile;

		@Parameter(names = { "--refresh", "-f" }, description = "Update all entries for the given paths.")
		boolean refreshPathCollisions = false;

		@Parameter(description = "A list of directories of files to add to the repository.")
		List<String> pathsToAdd;

		@Parameter(names = { "--parallel", "-p" }, description = "Use this many parallel threads to calculate checksums.")
		int parallel = 1;
	}
}
