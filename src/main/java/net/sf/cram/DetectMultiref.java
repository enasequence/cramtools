package net.sf.cram;

import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Scans a cram file and exits with exit code 1 if a multiref slice is found.
 * 
 * @author vadim
 *
 */
public class DetectMultiref {

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);
		File cramFile = new File(args[0]);
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		CramHeader header = CramIO.readCramHeader(is);
		Container c = null;
		while ((c = ContainerIO.readContainer(header.getVersion(), is)) != null && !c.isEOF()) {
			for (Slice slice : c.slices) {
				if (slice.sequenceId == Slice.MULTI_REFERENCE) {
					System.out.println("Read feature B detected.");
					System.exit(1);
				}
			}
		}
	}
}
