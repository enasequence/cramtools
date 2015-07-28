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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.Log;

public class Crambone {
	private static Log log = Log.getInstance(Crambone.class);
	private static File cramtoolsJar;
	private static String javaOpts;

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {
		Log.setGlobalLogLevel(Log.LogLevel.INFO);

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

		cramtoolsJar = params.jarFile;
		javaOpts = "-Xmx" + params.maxMem_MB + "m";

		log.info("Starting thread pool, size ", params.poolSize);
		BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(params.poolSize, params.poolSize, 1, TimeUnit.SECONDS,
				workQueue);

		Map<String, String> modelMap = new TreeMap<String, String>();
		Scanner modelScanner = new Scanner(params.modelFile);
		while (modelScanner.hasNextLine()) {
			String[] chunks = modelScanner.nextLine().split("\\t");
			modelMap.put(chunks[0], chunks[1]);
			log.info("Adding model " + chunks[0] + ": '" + chunks[1] + "'");
		}
		modelScanner.close();

		Scanner inputScanner = new Scanner(params.inputFile);
		while (inputScanner.hasNextLine()) {
			String pathname = inputScanner.nextLine();
			File bamFile = new File(pathname);

			String rel = null;
			if (params.bamRoot != null) {
				String b = bamFile.getParentFile().getAbsolutePath();
				String r = params.bamRoot.getAbsolutePath();
				if (!b.startsWith(r)) {
					log.error("BAM file path has different root: " + b);
					continue;
				}
				rel = b.substring(r.length());
			}

			for (String modelName : modelMap.keySet()) {
				String model = modelMap.get(modelName);

				List<Task> tasks = new ArrayList<Crambone.Task>();

				File destDir = new File(params.destDir, modelName);
				if (rel != null)
					destDir = new File(destDir, rel);
				destDir.mkdirs();
				Bam2CramTask bam2CramTask = new Bam2CramTask(destDir, bamFile, params.refFile, model);
				tasks.add(bam2CramTask);

				Cram2BamTask cram2BamTask = new Cram2BamTask(destDir, bam2CramTask.cramFile, params.refFile);
				tasks.add(cram2BamTask);

				Project p = new Project(params.cleanup, params.resetFailed, tasks);
				executor.submit(p);
			}

		}
		inputScanner.close();

		executor.shutdown();
		while (!executor.isTerminated()) {
			log.info("Pending tasks: ", workQueue.size());
			executor.awaitTermination(60, TimeUnit.SECONDS);
		}

		log.info("Done.");
	}

	@Parameters(commandDescription = "BAM to CRAM converter. ")
	static class Params {

		@Parameter(names = { "--cramtools-jar", "-J" }, required = false, converter = FileConverter.class, description = "File listing input BAM files, 'input' by default.")
		File jarFile = null;

		@Parameter(names = { "--input-file", "-I" }, required = false, converter = FileConverter.class, description = "File listing input BAM files, 'input' by default.")
		File inputFile = new File("input");

		@Parameter(names = { "--bam-root", "-B" }, required = false, converter = FileConverter.class, description = "A string that will be used as a root to get relative path from BAM file path.")
		File bamRoot;

		@Parameter(names = { "--model-file", "-M" }, required = false, converter = FileConverter.class, description = "File listing compression models, 'models' by default.")
		File modelFile = new File("models");

		@Parameter(names = { "--reference-fasta-file", "-R" }, required = true, converter = FileConverter.class, description = "Reference fasta file.")
		File refFile;

		@Parameter(names = { "--destination-directory", "-D" }, required = false, converter = FileConverter.class, description = "Destination directory, working directory by default.")
		File destDir = new File(System.getProperty("user.dir"));

		@Parameter(names = { "--cleanup", "-C" }, required = false, description = "Clean up and try again failed/garbled tasks.")
		boolean cleanup = false;

		@Parameter(names = { "--reset-failed" }, required = false, description = "Restart failed tasks from scratch.")
		boolean resetFailed = false;

		@Parameter(names = { "--pool-size", "-P" }, required = false, description = "Thread pool size. Number of cores by default.")
		int poolSize = Runtime.getRuntime().availableProcessors();

		@Parameter(names = { "--max-mem-MB", "-X" }, required = false, description = "-Xmx java option in megabytes.")
		int maxMem_MB = 4000;

		@Parameter(names = { "-h", "--help" }, required = false, description = "Print help and quit")
		boolean help = false;

	}

	private static class Project implements Runnable {
		private boolean cleanUp = false;
		private List<Task> tasks = new ArrayList<Crambone.Task>();
		private boolean resetIfFailed;

		public Project(boolean cleanUp, boolean resetIfFailed, List<Task> tasks) {
			this.cleanUp = cleanUp;
			this.resetIfFailed = resetIfFailed;
			this.tasks = tasks;
		}

		@Override
		public void run() {
			try {

				for (Task task : tasks) {
					switch (task.status()) {
					case SUCCESS:
					case NONE:

						break;

					default:
						if (resetIfFailed) {
							for (Task t : tasks)
								t.cleanUp();
						} else if (cleanUp) {
							task.cleanUp();
						} else {
							log.debug("Found failed task, skipping: ", task);
							return;
						}
					}
				}

				for (int i = 0; i < tasks.size(); i++) {
					Task task = tasks.get(i);

					Task.STATUS status = task.status();
					switch (status) {
					case SUCCESS:
						continue;
					case NONE:
						break;

					default:
						if (cleanUp) {
							for (int j = i; j < tasks.size(); j++) {
								Task t = tasks.get(i);
								if (!t.cleanUp()) {
									log.error("Cleanup failed: ", t.destDir.getAbsolutePath(), "/", t.fileNameBase);
									return;
								}
							}
						} else if (resetIfFailed) {
							for (Task t : tasks) {
								t.cleanUp();
							}
							return;
						} else {
							log.debug("Found failed task, skipping: ", task);
							return;
						}
						break;
					}

					task.run();
					if (task.exception != null) {
						log.error("Exception [", task.exception.getMessage(), "] ", task);
						return;
					}
					if (task.exitCode != 0) {
						log.error("Exit code [", task.exitCode, "] ", task);
						return;
					}
				}

				Task lastTask = tasks.get(tasks.size() - 1);
				if (lastTask.status() == Task.STATUS.SUCCESS)
					log.info("Success ", lastTask);
				else
					log.error("FAILED ", lastTask);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	private static class Bam2CramTask extends Task {
		private File bamFile;
		private File refFile;
		private String model;

		private String java = "java";
		private Log.LogLevel logLevel = Log.LogLevel.INFO;
		private String cramtoolsCommand = "cram";

		private File cramFile;

		private String cmd = null;

		public Bam2CramTask(File destDir, File bamFile, File refFile, String model) {
			super(bamFile.getName() + ".cram1", destDir);
			this.bamFile = bamFile;
			this.refFile = refFile;
			this.model = model;
			this.cramFile = new File(destDir, super.fileNameBase);
			outputFiles.add(cramFile);
		}

		@Override
		protected ProcessBuilder createProcessBuilder() {
			String cmd = String.format("%s %s -jar %s -l %s %s -I %s -R %s -O %s", java, javaOpts,
					cramtoolsJar.getAbsolutePath(), logLevel.name(), cramtoolsCommand, bamFile.getAbsolutePath(),
					refFile.getAbsolutePath(), cramFile.getAbsolutePath());
			if (model != null && model.length() > 0 && !model.matches("^\\s+$"))
				cmd += " -L " + model;

			log.info(cmd);

			List<String> list = new ArrayList<String>();
			list.add("/bin/sh");
			list.add("-c");
			list.add(cmd);

			return new ProcessBuilder(list);
		}

		@Override
		public String toString() {
			if (cmd != null)
				return cmd;
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" ").append(model);
			sb.append(" ").append(bamFile.getAbsolutePath());
			return sb.toString();
		}
	}

	private static class Cram2BamTask extends Task {
		private File cramFile;
		private File refFile;

		private String java = "java";
		private Log.LogLevel logLevel = Log.LogLevel.INFO;
		private String cramtoolsCommand = "bam";

		private File bamFile;

		private String cmd = null;

		public Cram2BamTask(File destDir, File cramFile, File refFile) {
			super(cramFile.getName() + ".bam", destDir);
			this.cramFile = cramFile;
			this.refFile = refFile;
			this.bamFile = new File(cramFile.getAbsolutePath() + ".bam");
			outputFiles.add(bamFile);
		}

		@Override
		protected ProcessBuilder createProcessBuilder() {
			String cmd = String.format("%s %s -jar %s -l %s %s -I %s -R %s -O %s", java, javaOpts,
					cramtoolsJar.getAbsolutePath(), logLevel.name(), cramtoolsCommand, cramFile.getAbsolutePath(),
					refFile.getAbsolutePath(), bamFile.getAbsolutePath());
			log.info(cmd);

			List<String> list = new ArrayList<String>();
			list.add("/bin/sh");
			list.add("-c");
			// for (String s : cmd.split(" "))
			// list.add(s);
			list.add(cmd);

			return new ProcessBuilder(list);
		}

		@Override
		public String toString() {
			if (cmd != null)
				return cmd;
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" ").append(cramFile.getAbsolutePath());
			return sb.toString();
		}
	}

	private static abstract class Task implements Runnable {
		public enum STATUS {
			NONE, GARBLED, INPROGRESS, FAILED, SUCCESS;
		}

		protected final String fileNameBase;

		protected final File destDir;
		protected final File inProgressMarkerFile;
		protected final File failedMarkerFile;
		protected final File successMarkerFile;
		protected final File outputFile;
		protected final File errorFile;
		protected List<File> outputFiles = new ArrayList<File>();

		private Exception exception;
		private int exitCode;

		private DateFormat dateFormat = DateFormat.getDateTimeInstance();

		public Task(String fileNameBase, File destDir) {
			this.fileNameBase = fileNameBase;
			this.destDir = destDir;

			inProgressMarkerFile = new File(destDir, fileNameBase + ".inprogress");
			failedMarkerFile = new File(destDir, fileNameBase + ".failed");
			successMarkerFile = new File(destDir, fileNameBase + ".success");
			outputFile = new File(destDir, fileNameBase + ".output");
			errorFile = new File(destDir, fileNameBase + ".error");
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(destDir.getAbsolutePath());
			sb.append(File.separator);
			sb.append(fileNameBase);
			return sb.toString();
		}

		protected abstract ProcessBuilder createProcessBuilder();

		protected void createAndWriteDefaultMessageToMarkerFile(File file) throws IOException {
			log.debug("Creating file ", file.getAbsolutePath());
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			try {
				fos.write(dateFormat.format(new Date()).getBytes());
			} finally {
				try {
					fos.close();
				} catch (Exception e) {
				}
			}
		}

		@Override
		public void run() {
			ProcessPump pump = null;
			OutputStream processOutput = null;
			OutputStream processError = null;
			try {
				if (status() != STATUS.NONE) {
					log.debug("Invalid task status: " + status());
					return;
				}
				exitCode = 0;

				log.debug("Starting task ", toString());

				createAndWriteDefaultMessageToMarkerFile(inProgressMarkerFile);

				ProcessBuilder b = createProcessBuilder();
				b.directory(destDir);
				// the following works only in java7, so we'll take a
				// b.redirectError(errorFile);
				// b.redirectOutput(outputFile);

				log.debug("Executing ", toString());

				Process process = b.start();
				processOutput = new BufferedOutputStream(new FileOutputStream(outputFile));
				processError = new BufferedOutputStream(new FileOutputStream(errorFile));
				pump = new ProcessPump(process, processOutput, processError);
				process.waitFor();
				exitCode = process.exitValue();
				process.destroy();

				for (Future<Long> f : pump.outputs) {
					try {
						log.debug("Process output size " + f.get());
					} catch (Exception e) {
						log.error("Process output pump exception.", e);
						throw e;
					}
				}

				if (exitCode == 0) {
					inProgressMarkerFile.delete();
					createAndWriteDefaultMessageToMarkerFile(successMarkerFile);
					log.debug("Completed ", toString());
					return;
				}

			} catch (Exception e) {
				exception = e;
			} finally {
				if (processOutput != null)
					try {
						processOutput.close();
					} catch (IOException e) {
					}
				if (processError != null)
					try {
						processError.close();
					} catch (IOException e) {
					}
			}

			try {
				createAndWriteDefaultMessageToMarkerFile(failedMarkerFile);
			} catch (IOException e) {
				exception = e;
			}

			if (exception != null)
				exception.printStackTrace();

			log.debug("Failed ", toString());
		}

		public boolean cleanUp() {
			log.debug("Cleaning up task ", fileNameBase);
			inProgressMarkerFile.delete();
			failedMarkerFile.delete();
			successMarkerFile.delete();
			outputFile.delete();
			errorFile.delete();

			if (outputFiles != null)
				for (File file : outputFiles)
					file.delete();

			return STATUS.NONE == status();
		}

		public STATUS status() {
			for (STATUS s : STATUS.values())
				switch (s) {
				case NONE:
					if (!inProgressMarkerFile.exists() && !failedMarkerFile.exists() && !successMarkerFile.exists())
						return STATUS.NONE;
					break;
				case INPROGRESS:
					if (inProgressMarkerFile.exists() && !failedMarkerFile.exists() && !successMarkerFile.exists())
						return STATUS.INPROGRESS;
					break;
				case FAILED:
					if (!inProgressMarkerFile.exists() && failedMarkerFile.exists() && !successMarkerFile.exists())
						return STATUS.FAILED;
					break;
				case SUCCESS:
					if (!inProgressMarkerFile.exists() && !failedMarkerFile.exists() && successMarkerFile.exists())
						return STATUS.SUCCESS;
					break;

				default:
					break;
				}
			return STATUS.GARBLED;
		}
	}

	private static class ProcessPump {
		ExecutorService es = Executors.newFixedThreadPool(2);
		List<Future<Long>> outputs = new ArrayList<Future<Long>>();

		ProcessPump(Process p, OutputStream stdout, OutputStream stderr) {
			if (stdout != null)
				outputs.add(es.submit(new Pump(p.getInputStream(), stdout)));
			if (stderr != null)
				outputs.add(es.submit(new Pump(p.getErrorStream(), stderr)));
		}

		private static class Pump implements Callable<Long> {
			InputStream is;
			OutputStream os;

			public Pump(InputStream is, OutputStream os) {
				this.is = is;
				this.os = os;
			}

			@Override
			public Long call() throws Exception {
				IOUtil.copyStream(is, os);
				return 0L;
			}

		}
	}
}
