package htsjdk.samtools.cram.paralell;

import java.util.concurrent.PriorityBlockingQueue;

final class OrderingConveyer<O extends IOrder> extends Conveyer<O> {
	private long order = 0;

	public OrderingConveyer() {
		super(new PriorityBlockingQueue<O>(), 0);
	}

	@Override
	public O tryAdvance() throws InterruptedException {
		O bb = queue.peek();
		if (bb == null || bb.order() > order) {
			return null;
		}

		if (bb.order() == order) {
			queue.take();
			order++;
			counter.incrementAndGet();
			return bb;
		}

		throw new RuntimeException(String.format("Expecting order %d but got ", order, bb.order()));
	}
}
