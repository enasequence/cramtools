package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class StreamPump extends Job {
	private static final Log log = Log.getInstance(StreamPump.class);
	InputStream is;
	OutputStream os;

	public StreamPump(InputStream is, OutputStream os) {
		this.is = is;
		this.os = os;
	}

	@Override
	protected void doRun() throws IOException {
		byte[] buf = new byte[4096];
		int len = is.read(buf);
		if (len == -1) {
			stop();
		} else {
			os.write(buf, 0, len);
		}
	}

	@Override
	protected void doFinish() throws Exception {
		log.info("stream pump out");
		os.close();
	}

}