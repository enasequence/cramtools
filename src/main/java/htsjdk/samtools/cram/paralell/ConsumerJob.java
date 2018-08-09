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

import java.util.function.Consumer;

class ConsumerJob<I> extends Job {
	private Conveyer<I> input;
	private Consumer<I> consumer;

	public ConsumerJob(Conveyer<I> input, Consumer<I> consumer) {
		this.input = input;
		this.consumer = consumer;
	}

	@Override
	protected void doRun() throws Exception {
		if (input.hasCompleted()) {
			stop();
			return;
		}

		I object = input.tryAdvance();
		if (object == null) {
			return;
		}
		consumer.accept(object);
	}
}
