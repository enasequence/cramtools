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

import java.util.function.Function;

class TransformerJob<I, O> extends Job {
	private Conveyer<I> inQueue;
	private Conveyer<O> outQueue;
	private Function<I, O> function;

	public TransformerJob(Conveyer<I> inQueue, Conveyer<O> outQueue, Function<I, O> function) {
		this.inQueue = inQueue;
		this.outQueue = outQueue;
		this.function = function;
		outQueue.addSupplier();
	}

	@Override
	protected void doRun() throws Exception {
		if (inQueue.hasCompleted()) {
			stop();
			return;
		}

		I input = inQueue.tryAdvance();
		if (input == null) {
			return;
		}
		O output = function.apply(input);
		outQueue.put(output);
	}

	@Override
	protected void doFinish() throws Exception {
		outQueue.close();
	}

}
