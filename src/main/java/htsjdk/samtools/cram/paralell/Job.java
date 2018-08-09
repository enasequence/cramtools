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

import htsjdk.samtools.util.Log;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class Job implements Runnable {
	private static Log log = Log.getInstance(Job.class);
	private AtomicBoolean stop = new AtomicBoolean(false);
	private AtomicBoolean done = new AtomicBoolean(false);
	private Exception exception;
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Exception getException() {
		return exception;
	}

	public void stop() {
		log.info("Job done: " + getClass().getName() + ", name: " + getName());
		stop.set(true);
	}

	public boolean isDone() {
		return done.get();
	}

	@Override
	public void run() {
		log.info("Starting job: " + getClass().getName() + ", name: " + getName());
		while (!stop.get()) {
			try {
				doRun();
				Thread.yield();

			} catch (Exception e) {
				e.printStackTrace();
				stop();
			}
		}
		done.set(true);
		log.info("Finished job: " + getClass().getName() + ", name: " + getName());
		try {
			doFinish();
		} catch (Exception exception) {
			exception.printStackTrace();
			this.exception = exception;
			stop();
		}
	}

	protected abstract void doRun() throws Exception;

	protected void doFinish() throws Exception {

	}
}