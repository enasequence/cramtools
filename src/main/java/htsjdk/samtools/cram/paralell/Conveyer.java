package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.util.Log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class similar to a blocking queue that can be closed. Conveyer is closed
 * when all registered suppliers have called close() method and the underlying
 * queue is empty. Effectively this should guarantee that no new objects will
 * appear on the conveyer and all consumers should shutdown normally.
 * 
 * @author vadim
 *
 * @param <T>
 */
class Conveyer<T> {
	private static Log log = Log.getInstance(Conveyer.class);
	protected BlockingQueue<T> queue;
	private AtomicInteger countDownToClose = new AtomicInteger(0);
	protected AtomicInteger counter = new AtomicInteger(0);

	/**
	 * Create a new conveyer with the given number of suppliers.
	 * 
	 * @param queue
	 *            a delegate queue to use
	 * @param nofSuppliers
	 *            number of suppliers to the conveyer
	 */
	Conveyer(BlockingQueue<T> queue, int nofSuppliers) {
		this.queue = queue;
		this.countDownToClose.set(nofSuppliers);
	}

	static <T> Conveyer<T> create(int queueCapacity, int suppliers) {
		return new Conveyer<T>(new ArrayBlockingQueue<T>(queueCapacity), suppliers);
	}

	static <T> Conveyer<T> createWithQueueCapacity(int queueCapacity) {
		return new Conveyer<T>(new ArrayBlockingQueue<T>(queueCapacity), 0);
	}

	void close() {
		log.info("close: " + countDownToClose.decrementAndGet());
	}

	void addSupplier() {
		countDownToClose.incrementAndGet();
	}

	/**
	 * Check if the conveyer has processed everything.
	 * 
	 * @return true if there nothing else to process, false otherwise
	 */
	boolean hasCompleted() {
		return countDownToClose.get() <= 0 && queue.isEmpty();
	}

	int size() {
		return queue.size();
	}

	boolean isEmpty() {
		return queue.isEmpty();
	}

	void put(T object) throws InterruptedException {
		if (hasCompleted())
			throw new IllegalStateException();
		queue.put(object);
	}

	T peek() {
		return queue.peek();
	}

	int remainingCapacity() {
		return queue.remainingCapacity();
	}

	/**
	 * Try to pop an object from the conveyer. This is a semi-blocking
	 * implementation: it repeatedly blocks and checks if the conveyer has
	 * completed.
	 * 
	 * @return an object to process or null if everything has been processed
	 * @throws InterruptedException
	 *             as per Java IO contract due to blocking queue polling.
	 */
	T tryAdvance() throws InterruptedException {
		if (hasCompleted())
			return null;

		T poll = null;
		while (!hasCompleted() && (poll = queue.poll(100, TimeUnit.MILLISECONDS)) == null)
			;
		if (poll == null)
			return null;

		counter.incrementAndGet();
		return poll;
	}

	@Override
	public String toString() {
		return String.format("[%s, suppliers=%d, queued=%d, processed=%d]", hasCompleted() ? "completed" : "active",
				countDownToClose.get(), queue.size(), counter.get());
	}
}
