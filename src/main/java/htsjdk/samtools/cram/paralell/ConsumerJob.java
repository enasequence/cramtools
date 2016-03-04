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
