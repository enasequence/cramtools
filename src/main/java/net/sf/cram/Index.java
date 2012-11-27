package net.sf.cram;

import java.io.IOException;
import java.io.OutputStream;

import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;

public class Index {
	private OutputStream os;

	public Index(OutputStream os) {
		this.os = os;
	}

	public void addContainer(Container c, long offset) throws IOException {
		int i = 0;
		for (Slice s : c.slices) {
			String string = String.format("%d\t%d\t%d\t%d\t%d\n", c.sequenceId,
					s.alignmentStart, s.nofRecords, offset, c.landmarks[i++]);
			os.write(string.getBytes());
		}
	}

	public void close() throws IOException {
		os.close();
	}
}
