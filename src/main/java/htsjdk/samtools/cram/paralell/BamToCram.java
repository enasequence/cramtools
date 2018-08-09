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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.CramLossyOptions;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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

public class BamToCram {
	static Log log = Log.getInstance(BamToCram.class);

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
		InputStream bamInputStream = new BufferedInputStream(params.bamFile == null ? System.in : new FileInputStream(
				params.bamFile));
		OutputStream cramOutputStream = params.outputCramFile == null ? System.out : new FileOutputStream(
				params.outputCramFile);

		if (params.threads == 0) {
			params.threads = Math.max(5, Runtime.getRuntime().availableProcessors());
		} else if (params.threads < 5) {
			System.err.println("Too few threads: minimum 4 threads required. ");
			System.exit(1);
		}

		final int threadsInThePool = params.threads - 1;
		final int bgzfUncompressionThreads = 1;
		final int cramWritingThreads = 1;
		final int bamSlicingThreads = 1;
		final int conversionThreads = threadsInThePool - bgzfUncompressionThreads - cramWritingThreads
				- bamSlicingThreads;
		final int queuesCapacity = conversionThreads * 2;

		log.info(String.format("thread pool size=%d, converion threads=%d, queues capacity=%d", threadsInThePool,
				conversionThreads, queuesCapacity));
		log.info("Creating thread pool with size " + threadsInThePool);
		ThreadPoolExecutor executor = new ThreadPoolExecutor(threadsInThePool, threadsInThePool, 60L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(threadsInThePool * 2), new CallerRunsPolicy());

		int bufSize = 1024 * 1024;
		PipedOutputStream uncompressedBamOutputStream = new PipedOutputStream();
		PipedInputStream uncompressedBamInputStream = new PipedInputStream(uncompressedBamOutputStream, bufSize);

		StreamPump BGZF_uncompressionPump = new StreamPump(new BlockCompressedInputStream(bamInputStream),
				uncompressedBamOutputStream);
		BGZF_uncompressionPump.setName("BGZF_UNC_PUMP");
		executor.execute(BGZF_uncompressionPump);

		SAMFileHeader samFileHeader = SAMFileHeader_Utils.readHeader(new BinaryCodec(uncompressedBamInputStream),
				ValidationStringency.SILENT, null);
		CramHeader cramHeader = new CramHeader(CramVersions.CRAM_v3, new File(args[0]).getName(), samFileHeader);
		CramIO.writeCramHeader(cramHeader, cramOutputStream);

		Conveyer<OrderedByteArray> bam_OBA_conveyer = Conveyer.createWithQueueCapacity(queuesCapacity);
		SupplierJob<OrderedByteArray> bam_OBA_supplier = new SupplierJob<OrderedByteArray>(bam_OBA_conveyer,
				new Bam_OBA_Supplier(new BufferedInputStream(uncompressedBamInputStream)));
		bam_OBA_supplier.setName("BAM_SLICER_SUPPLIER");

		Conveyer<OrderedByteArray> cram_OBA_conveyer = new OrderingConveyer<OrderedByteArray>();

		List<Job> converterJobs = new ArrayList<Job>();
		CramLossyOptions lossyOptions = CramLossyOptions.lossless();
		for (int i = 0; i < conversionThreads; i++) {
			BamToCram_OBA_Function convertFunction = new BamToCram_OBA_Function(cramHeader, referenceSource,
					lossyOptions);
			convertFunction.setCaptureTags(params.captureTags);
			convertFunction.setIgnoreTags(params.ignoreTags);
			Job job = new TransformerJob<OrderedByteArray, OrderedByteArray>(bam_OBA_conveyer, cram_OBA_conveyer,
					convertFunction);
			job.setName("BC_CONVERTER_" + i);
			converterJobs.add(job);
		}

		executor.execute(bam_OBA_supplier);
		for (Job job : converterJobs) {
			executor.execute(job);
		}

		ConsumerJob<OrderedByteArray> cram_OBA_writeJob = new ConsumerJob<OrderedByteArray>(cram_OBA_conveyer,
				new OBAWriteConsumer(cramOutputStream));
		cram_OBA_writeJob.setName("CRAM_BYTE_WRITE_JOB");
		executor.execute(cram_OBA_writeJob);

		long time = System.currentTimeMillis();
		while (!cram_OBA_writeJob.isDone()) {
			Thread.sleep(100);
			if (System.currentTimeMillis() - time > 1000) {
				log.info(String.format("BAM_OBA %s; CRAM_OBA %s", bam_OBA_conveyer.toString(),
						cram_OBA_conveyer.toString()));
				time = System.currentTimeMillis();
			}
		}

		executor.shutdown();
		CramIO.issueEOF(cramHeader.getVersion(), cramOutputStream);
		cramOutputStream.close();
	}

	@Parameters(commandDescription = "BAM to CRAM multithreaded converter. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-bam-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM file to be converted to CRAM. Omit if standard input (pipe).")
		File bamFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFasta;

		@Parameter(names = { "--output-cram-file", "-O" }, converter = FileConverter.class, description = "The path for the output CRAM file. Omit if standard output (pipe).")
		File outputCramFile = null;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--ignore-tags" }, description = "Ignore the tags listed, for example 'OQ:XA:XB'")
		String ignoreTags = "";

		@Parameter(names = { "--capture-tags" }, description = "Capture the tags listed, for example 'OQ:XA:XB'")
		String captureTags = "";

		@Parameter(names = { "--capture-all-tags" }, description = "Capture all tags.")
		boolean captureAllTags = false;

		@Parameter(names = { "--threads" }, description = "Number of threads to use (minimum 5; use 0 for number of available cores.")
		public int threads = 5;
	}
}
