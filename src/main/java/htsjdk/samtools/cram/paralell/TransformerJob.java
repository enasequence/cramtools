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
