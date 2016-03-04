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
