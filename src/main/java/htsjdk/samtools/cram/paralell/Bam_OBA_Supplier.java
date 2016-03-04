package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.samtools.util.RuntimeIOException;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Supplier;

class Bam_OBA_Supplier implements Supplier<OrderedByteArray> {
	private Log log = Log.getInstance(Bam_OBA_Supplier.class);
	private BufferedInputStream is;
	private long order = 0;
	private BinaryCodec codec;
	private ByteArrayOutputStream baos;
	private int refId;
	private int recordCounter;

	public Bam_OBA_Supplier(BufferedInputStream is) {
		this.is = is;
		codec = new BinaryCodec();
		codec.setInputStream(is);
		baos = new ByteArrayOutputStream();
		refId = Integer.MIN_VALUE;
		recordCounter = 0;
	}

	@Override
	public OrderedByteArray get() {
		while (true) {
			try {
				is.mark(8);
				int recordLength = 0;
				try {
					recordLength = codec.readInt();
				} catch (RuntimeEOFException e) {
					// seems legit:
					if (recordCounter == 0)
						return null;
					return flushStripe(baos);
				}

				if (recordLength < 8 * 4) {
					Thread.dumpStack();
					throw new SAMFormatException("Invalid record length: " + recordLength);
				}

				final int referenceID = codec.readInt();
				is.reset();
				if (refId != referenceID && refId != Integer.MIN_VALUE && baos.size() > 0) {
					return flushStripe(baos);
				}

				refId = referenceID;
				byte[] recordData = InputStreamUtils.readFully(is, recordLength + 4);
				baos.write(recordData);
				if (recordCounter++ >= 10000 - 1) {
					return flushStripe(baos);
				}
			} catch (IOException e) {
				throw new RuntimeIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private OrderedByteArray flushStripe(ByteArrayOutputStream baos) throws InterruptedException, IOException {
		OrderedByteArray stripe = new OrderedByteArray();
		stripe.bytes = baos.toByteArray();
		log.debug(String.format("adding stripe: order=%d, ref=%d, records=%d, bytes=%d", order, refId, recordCounter,
				stripe.bytes.length));
		stripe.order = order++;
		baos.reset();
		recordCounter = 0;
		return stripe;
	}
}