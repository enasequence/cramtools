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

import java.util.function.Supplier;

class SupplierJob<O> extends Job {
	private Conveyer<O> outQueue;
	private Supplier<O> supplier;

	public SupplierJob(Conveyer<O> outQueue, Supplier<O> supplier) {
		this.outQueue = outQueue;
		this.supplier = supplier;
		outQueue.addSupplier();
	}

	@Override
	protected void doRun() throws Exception {
		if (outQueue.remainingCapacity() == 0) {
			Thread.sleep(100);
		}
		O object = supplier.get();
		if (object == null) {
			stop();
		} else {
			outQueue.put(object);
		}
	}

	@Override
	protected void doFinish() throws Exception {
		outQueue.close();
	}

}
