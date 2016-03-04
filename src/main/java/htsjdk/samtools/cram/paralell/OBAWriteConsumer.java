package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.util.RuntimeIOException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

class OBAWriteConsumer implements Consumer<OrderedByteArray> {
	private OutputStream os;

	public OBAWriteConsumer(OutputStream os) {
		this.os = os;
	}

	@Override
	public void accept(OrderedByteArray t) {
		try {
			os.write(t.bytes);
		} catch (IOException e) {
			throw new RuntimeIOException(e);
		}
	}
}
