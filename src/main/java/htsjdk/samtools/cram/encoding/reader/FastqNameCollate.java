package htsjdk.samtools.cram.encoding.reader;

import java.io.IOException;
import java.io.OutputStream;

public class FastqNameCollate extends NameCollate<FastqRead> {
	private OutputStream[] streams;
	private OutputStream overspillStream;
	private long kicked = 0, ready = 0, total = 0;

	public FastqNameCollate(OutputStream[] streams, OutputStream overspillStream) {
		super();
		this.streams = streams;
		this.overspillStream = overspillStream;
	}

	@Override
	protected boolean needsCollating(FastqRead read) {
		return read.templateIndex > 0;
	}

	@Override
	protected void ready(FastqRead read) {
		try {
			streams[read.templateIndex].write(read.data);
			ready++;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void kickedFromCache(FastqRead read) {
		try {
			overspillStream.write(read.data);
			kicked++;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void add(FastqRead read) {
		super.add(read);
		total++;
	}

	public String report() {
		return String.format("ready: %d, kicked %d, total: %d.", ready, kicked, total);
	}
}
