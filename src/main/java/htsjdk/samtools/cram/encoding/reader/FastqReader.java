package htsjdk.samtools.cram.encoding.reader;

import net.sf.cram.common.Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;


public class FastqReader implements Iterator<FastqRead> {
	private InputStream is;
	private boolean stripSegmentIndexFromReadName = true;
	private FastqRead next;
	private int indexInSegment = 0;

	public FastqReader(InputStream is) throws IOException {
		this.is = is;
		is.mark(10);
		if (is.read() == -1) {
			next = null;
		} else {
			is.reset();
			next = readNext();
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	private byte[] readName() throws IOException {
		int nameLen = 0;

		is.mark(1024);
		int b = is.read();
		for (; b != -1 && b != '\n'; nameLen++, b = is.read())
			;
		if (b == -1)
			throw new EOFException("Unexpected end of fastq stream.");

		is.reset();

		byte[] name = null;
		if (stripSegmentIndexFromReadName) {
			is.skip(nameLen - 2);
			switch (is.read()) {
			case -1:
				throw new EOFException("Unexpected end of fastq stream.");
			case '/':
				indexInSegment = is.read();
				if (indexInSegment == -1)
					throw new EOFException("Unexpected end of fastq stream.");

				if (indexInSegment > 0) {
					is.reset();
					name = Utils.readFully(is, nameLen - 2);
					// a slash, a digit and a new line:
					is.skip(3);
				} else
					indexInSegment = 0;
				break;

			default:
				break;
			}
		}

		if (name == null) {
			is.reset();
			name = Utils.readFully(is, nameLen);
			// skip new line byte:
			is.skip(1);
		}

		return name;
	}
	private byte[] readBases() throws IOException {
		int readLen = 0;

		is.mark(1024 * 1024);
		int b = is.read();
		for (; b != -1 && b != '\n'; readLen++, b = is.read())
			;
		if (b == -1)
			throw new EOFException("Unexpected end of fastq stream.");

		is.reset();

		byte[] bases = Utils.readFully(is, readLen);
		// skip new line byte:
		is.skip(1);

		return bases;
	}

	private FastqRead readNext() throws IOException {
		is.mark(10);
		if (is.read() == -1)
			return null;
		else
			is.reset();

		byte[] name = readName();
		byte[] bases = readBases();
		// skip scores defintion line: '+', new line:
		is.skip(1 + 1);
		byte[] scores = Utils.readFully(is, bases.length);
		// skip new line byte at the end of scores line:
		is.skip(1);

		return new FastqRead(bases.length, name, true, indexInSegment, bases, scores);
	}

	@Override
	public FastqRead next() {
		if (next == null)
			throw new RuntimeException("No elements left.");
		FastqRead result = next;
		try {
			next = readNext();
			return result;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {
		throw new RuntimeException("Remove not supported.");
	}

	public static void main(String[] args) throws IOException {
		String r1 = "n1\nACGT\n+\n!!!!\n";
		String r2 = "n2/1\nCGT\n+\n!!!\n";
		String r3 = "n2/2\nCGT\n+\n!!!\n";

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(r1.getBytes());
		baos.write(r2.getBytes());
		baos.write(r3.getBytes());
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

		FastqReader reader = new FastqReader(bais);
		while (reader.hasNext()) {
			FastqRead read = reader.next();
			System.out.println(new String(read.data));
		}

	}

}
