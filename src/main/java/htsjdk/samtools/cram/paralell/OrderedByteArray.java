package htsjdk.samtools.cram.paralell;

class OrderedByteArray implements Comparable<OrderedByteArray>, IOrder {
	public byte[] bytes;
	public long order = 0;

	@Override
	public int compareTo(OrderedByteArray o) {
		return (int) (order - o.order);
	}

	@Override
	public String toString() {
		return String.format("order=%d, data length=%d", order, bytes == null ? -1 : bytes.length);
	}

	@Override
	public long order() {
		return order;
	}
}