/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import net.sf.cram.Bam2Cram;
import net.sf.cram.CramTools;
import net.sf.cram.ref.ReferenceSource;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class CramToBam {
	static Log log = Log.getInstance(CramToBam.class);

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException, InterruptedException {
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

		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				System.err.println("Exception in thread " + t);
				e.printStackTrace();
				System.exit(1);
			}
		});
		Log.setGlobalLogLevel(LogLevel.INFO);
		InputStream cramInputStream = new BufferedInputStream(params.cramFile == null ? System.in
				: new FileInputStream(params.cramFile));
		OutputStream bamOutputStream = params.outputBamFile == null ? System.out : new FileOutputStream(
				params.outputBamFile);

		if (params.threads == 0) {
			params.threads = Math.max(4, Runtime.getRuntime().availableProcessors());
		} else if (params.threads < 4) {
			System.err.println("Too few threads: minimum 4 threads required. ");
			System.exit(1);
		}

		final int threadsInThePool = params.threads - 1;
		final int cramContainerSupplierThreads = 1;
		final int bamObaConsumerThreads = 1;
		final int conversionThreads = threadsInThePool - cramContainerSupplierThreads - bamObaConsumerThreads;
		final int queuesCapacity = conversionThreads * 2;

		log.info(String.format("thread pool size=%d, converion threads=%d, queues capacity=%d", threadsInThePool,
				conversionThreads, queuesCapacity));

		CramHeader cramHeader = CramIO.readCramHeader(cramInputStream);
		CramContainer_OBA_Supplier container_OBA_Supplier = new CramContainer_OBA_Supplier(cramInputStream, cramHeader);
		Conveyer<OrderedByteArray> cramContainer_OBA_conveyer = Conveyer.createWithQueueCapacity(queuesCapacity);
		SupplierJob<OrderedByteArray> cramContainer_OBA_SupplierJob = new SupplierJob<OrderedByteArray>(
				cramContainer_OBA_conveyer, container_OBA_Supplier);

		Conveyer<OrderedByteArray> bam_OBA_conveyer = new OrderingConveyer<OrderedByteArray>();

		List<Job> converterJobs = new ArrayList<Job>();
		for (int i = 0; i < conversionThreads; i++) {
			CramToBam_OBA_Function f = new CramToBam_OBA_Function(cramHeader, referenceSource);
			TransformerJob<OrderedByteArray, OrderedByteArray> job = new TransformerJob<OrderedByteArray, OrderedByteArray>(
					cramContainer_OBA_conveyer, bam_OBA_conveyer, f);
			converterJobs.add(job);
		}

		BlockCompressedOutputStream blockOS = new BlockCompressedOutputStream(bamOutputStream, null);
		BinaryCodec outputBinaryCodec = new BinaryCodec();
		outputBinaryCodec.setOutputStream(blockOS);
		SAMFileHeader_Utils.writeHeader(outputBinaryCodec, cramHeader.getSamFileHeader());
		blockOS.flush();

		OBAWriteConsumer bam_OBA_writeConsumer = new OBAWriteConsumer(bamOutputStream);
		ConsumerJob<OrderedByteArray> bam_OBA_writeJob = new ConsumerJob<OrderedByteArray>(bam_OBA_conveyer,
				bam_OBA_writeConsumer);

		log.info("Creating thread pool with size " + threadsInThePool);
		ThreadPoolExecutor executor = new ThreadPoolExecutor(threadsInThePool, threadsInThePool, 60L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(threadsInThePool * 2), new CallerRunsPolicy());
		executor.execute(cramContainer_OBA_SupplierJob);
		for (Job job : converterJobs)
			executor.execute(job);
		executor.execute(bam_OBA_writeJob);

		long time = System.currentTimeMillis();
		while (!bam_OBA_writeJob.isDone()) {
			Thread.sleep(100);
			if (System.currentTimeMillis() - time > 1000) {
				log.info(String.format("CRAM_OBA %s; BAM_OBA %s", cramContainer_OBA_conveyer.toString(),
						bam_OBA_conveyer.toString()));
				time = System.currentTimeMillis();
			}
		}

		executor.shutdown();
		bamOutputStream.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
		bamOutputStream.close();
	}

	@Parameters(commandDescription = "BAM to CRAM multithreaded converter. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-cram-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM file to be converted to CRAM. Omit if standard input (pipe).")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFasta;

		@Parameter(names = { "--output-bam-file", "-O" }, converter = FileConverter.class, description = "The path for the output CRAM file. Omit if standard output (pipe).")
		File outputBamFile = null;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--threads" }, description = "Number of threads to use (minimum 4; use 0 for number of available cores.")
		public int threads = 4;
	}
}
