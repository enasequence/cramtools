package htsjdk.samtools.cram.structure;

import java.io.IOException;
import java.io.InputStream;

public class SliceIO_Accessor {

	public static void readIntoSliceFromStream(final int major, final Slice slice, final InputStream inputStream) throws IOException {
		SliceIO.read(major, slice, inputStream);
	}

}
