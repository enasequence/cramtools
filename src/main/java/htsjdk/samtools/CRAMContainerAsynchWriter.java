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

package htsjdk.samtools;

import htsjdk.samtools.cram.CramSerilization;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.cram.ref.ReferenceSource;

public class CRAMContainerAsynchWriter extends CRAMContainerStreamWriter {
	private ThreadPoolExecutor es;
	private long batchCounter = 0;
	private PriorityBlockingQueue<Batch> batchResultQueue;
	private long indexingCounter = 0;
	private BlockingQueue<Runnable> jobQueue;
	private List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());

	public CRAMContainerAsynchWriter(OutputStream outputStream, OutputStream indexStream, ReferenceSource source,
			SAMFileHeader samFileHeader, String cramId, int threadPoolSize) {
		super(outputStream, indexStream, source, samFileHeader, cramId);

		if (threadPoolSize < 1)
			throw new IllegalArgumentException("Need at least 1 worker thread for asynch CRAM writing.");

		int maxJobs = threadPoolSize * 2;
		jobQueue = new ArrayBlockingQueue<Runnable>(maxJobs);
		// given the rejected execution policy an extra slot needed:
		batchResultQueue = new PriorityBlockingQueue<CRAMContainerAsynchWriter.Batch>(maxJobs + 1);

		System.out.println("Starting thread pool max size: " + threadPoolSize);
		es = new ThreadPoolExecutor(1, threadPoolSize, 10, TimeUnit.SECONDS, jobQueue);
		es.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		postProcessorThread = new Thread(postProcessor);
		postProcessorThread.start();
	}

	private void exceptionInWorker(Throwable t) {
		try {
			t.printStackTrace();
			es.shutdown();
			batchResultQueue.clear();
			jobQueue.clear();
			exceptions.add(t);
		} catch (Exception e) {
			// can't do much but whine:
			e.printStackTrace();
		}
	}

	@Override
	protected void flushContainer() throws IllegalArgumentException, IllegalAccessException, IOException {
		if (!exceptions.isEmpty()) {
			throw new RuntimeException(exceptions.get(0));
		}

		if (samRecords.isEmpty())
			return;

		Batch batch = new Batch(Arrays.asList(samRecords.toArray(new SAMRecord[samRecords.size()])));
		CRAMContainerAsynchWriter.this.samRecords.clear();
		es.execute(batch);
		System.out.printf("Convert jobs: %d, write jobs: %d.\n", jobQueue.size(), batchResultQueue.size());
	}

	@Override
	public void finish(boolean writeEOFContainer) {
		if (!exceptions.isEmpty()) {
			throw new RuntimeException(exceptions.get(0));
		}

		try {

			flushContainer();
			while (!jobQueue.isEmpty()) {
				Thread.sleep(1000);
			}
			System.out.println("job queue empty");
			es.shutdown();
			while (!es.isTerminated()) {
				Thread.sleep(1000);
			}
			System.out.println("terminated");

			while (!batchResultQueue.isEmpty()) {
				Thread.sleep(1000);
			}
			System.out.println("result queue empty");
			postProcessorThread.interrupt();

			if (writeEOFContainer) {
				CramIO.issueEOF(cramVersion, outputStream);
			}
			outputStream.flush();
			if (indexer != null) {
				indexer.finish();
			}
			outputStream.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private class Batch implements Runnable, Comparable<Batch> {
		List<SAMRecord> records;
		long ordinal = batchCounter++;
		Container container;
		byte[] bytes;

		public Batch(List<SAMRecord> records) {
			this.records = records;
		}

		@Override
		public void run() {
			try {
				container = CramSerilization.convert(records, samFileHeader, source, lossyOptions);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ContainerIO.writeContainer(cramVersion, container, baos);
				bytes = baos.toByteArray();
				batchResultQueue.put(this);
			} catch (Exception e) {
				exceptionInWorker(e);
			}
		}

		@Override
		public int compareTo(Batch o) {
			return (int) (ordinal - o.ordinal);
		}
	}

	private Runnable postProcessor = new Runnable() {

		@Override
		public void run() {
			try {
				Batch batch = null;
				while (!Thread.interrupted()) {
					batch = batchResultQueue.peek();
					if (batch == null || batch.ordinal > indexingCounter) {
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							break;
						}
						continue;
					}

					batch = batchResultQueue.take();
					if (batch.ordinal != indexingCounter)
						throw new RuntimeException("Batch out of order.");

					indexingCounter++;
					batch.container.offset = offset;
					offset += batch.bytes.length;
					outputStream.write(batch.bytes);
					if (indexer != null)
						indexer.processContainer(batch.container, ValidationStringency.SILENT);
				}
			} catch (Exception e) {
				exceptionInWorker(e);
			}
		}
	};
	private Thread postProcessorThread;
}
