package net.sf.cram.fasta;

import java.io.File;
import java.io.IOException;

import net.sf.picard.util.Log;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.SeekableFileStream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class IndexFasta {
	private static Log log = Log.getInstance(IndexFasta.class);

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		BlockCompressedInputStream bcis = new BlockCompressedInputStream(new SeekableFileStream(params.file));
		bcis.available();
		MultiLineIndexer mli = new MultiLineIndexer(bcis);

		FAIDX_FastaIndexEntry e;
		while (!System.out.checkError() && (e = mli.readNext()) != null) {
			System.out.println(e);
		}
	}

	@Parameters
	static class Params {
		@Parameter(names = { "-I" }, converter = FileConverter.class)
		File file;

	}
}
