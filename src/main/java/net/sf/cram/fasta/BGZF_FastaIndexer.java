package net.sf.cram.fasta;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Log;

class BGZF_FastaIndexer {
	private static Log log = Log.getInstance(BGZF_FastaIndexer.class);
	private BlockCompressedInputStream is;
	private long start;
	private int len;
	private int lineWidthNoNL, lineWidthWithNL;
	private ByteBuffer lineBuf = ByteBuffer.allocate(1024);
	private long lineCounter = 0;
	private int sequenceCounter = 0;
	private boolean hasNextNameInBuf = false;

	public BGZF_FastaIndexer(BlockCompressedInputStream is) {
		this.is = is;
	}

	private boolean readLine() throws IOException {
		lineCounter++;
		int ch = is.read();
		if (ch == -1)
			return false;

		lineBuf.clear();
		lineBuf.put((byte) (0xFF & ch));
		while ((ch = is.read()) != -1) {
			lineBuf.put((byte) (0xFF & ch));
			if (!lineBuf.hasRemaining())
				reallocate();
			if (ch == '\n')
				break;

		}

		if (ch == -1)
			throw new EOFException();

		lineBuf.flip();

		return true;
	}

	private void reallocate() {
		int newSize = Math.min(2 * lineBuf.capacity(), Integer.MAX_VALUE);
		if (newSize <= lineBuf.capacity())
			throw new RuntimeException("Can't handle lines longer than 2gb.");

		log.info("Reallocating line buffer to new size: " + newSize);

		int pos = lineBuf.position();
		byte[] newArray = new byte[newSize];
		System.arraycopy(lineBuf.array(), 0, newArray, 0, lineBuf.limit());
		lineBuf = ByteBuffer.wrap(newArray);
		lineBuf.position(pos);
		lineBuf.limit(newSize);
	}

	private int trimmedLength() {
		int len = lineBuf.limit();
		for (int i = lineBuf.limit() - 1; i >= 0; i--) {
			switch (lineBuf.get(i)) {
			case '\r':
			case '\n':
				len--;
				break;

			default:
				break;
			}
		}
		return len;
	}

	private void readSeq() throws IOException {
		len = 0;
		lineWidthNoNL = 0;
		lineWidthWithNL = 0;

		start = is.getFilePointer();
		hasNextNameInBuf = false;
		while (readLine()) {
			if (lineBuf.get(0) == '>') {
				hasNextNameInBuf = true;
				break;
			}

			lineWidthWithNL = Math.max(lineWidthWithNL, lineBuf.limit());
			int trimmedLength = trimmedLength();
			lineWidthNoNL = Math.max(lineWidthNoNL, trimmedLength);

			len += trimmedLength;
		}

		if (len == 0)
			throw new RuntimeException("Invalid format: no sequence line.");
	}

	public FAIDX_FastaIndexEntry readNext() throws IOException {
		if (!hasNextNameInBuf)
			if (!readLine())
				return null;

		if (lineBuf.limit() == 0)
			throw new RuntimeException("Invalid format: empty line.");

		if (lineBuf.get(0) != '>') {
			throw new RuntimeException("Invalid format: sequence name expected to start with '>' at line "
					+ lineCounter);

		}

		byte[] nameBytes = new byte[trimmedLength() - 1];
		lineBuf.limit(nameBytes.length + 1);
		lineBuf.get();
		lineBuf.get(nameBytes);

		readSeq();

		String name = new String(nameBytes).split(" ")[0];

		return new FAIDX_FastaIndexEntry(sequenceCounter++, name, len, start, lineWidthNoNL, lineWidthWithNL);
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		for (File file : params.files) {
			log.info("Indexing file: " + file.getAbsolutePath());
			BlockCompressedInputStream bcis = new BlockCompressedInputStream(new SeekableFileStream(file));
			bcis.available();
			BGZF_FastaIndexer mli = new BGZF_FastaIndexer(bcis);

			PrintWriter writer = new PrintWriter(file.getAbsolutePath() + ".fai");

			FAIDX_FastaIndexEntry e;
			while (!writer.checkError() && (e = mli.readNext()) != null)
				writer.println(e);

			writer.close();
		}
	}

	@Parameters
	static class Params {
		@Parameter
		List<File> files;
	}
}