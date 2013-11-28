package net.sf.cram.fasta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import net.sf.cram.io.ByteBufferUtils;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.SeekableFileStream;

import org.bouncycastle.util.Arrays;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class AccessFasta {

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		File indexFile = findIndexFile(params.file);
		if (indexFile == null) {
			System.err.println("Index file not found.");
			System.exit(1);
		}

		if (params.test) {
			testAllEntries(params.file, indexFile);
			return;
		}

		BlockCompressedInputStream bcis = new BlockCompressedInputStream(new SeekableFileStream(params.file));
		bcis.available();
		Scanner scanner = new Scanner(indexFile);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] values = line.split("\t");

			String name = values[0];
			if (!params.names.contains(name))
				continue;

			int len = Integer.valueOf(values[1]);
			long startPointer = Long.valueOf(values[2]);
			int lineWidthNoNL = Integer.valueOf(values[3]);
			int lineWidthWithNL = Integer.valueOf(values[4]);

			bcis.seek(startPointer);
			int lines = len / lineWidthNoNL + (len % lineWidthNoNL == 0 ? 0 : 1);
			int separatorLen = lineWidthWithNL - lineWidthNoNL;
			int bytesToRead = len + separatorLen * lines - 1;

			byte[] data = new byte[bytesToRead];
			int read = ByteBufferUtils.readFully(data, bcis);
			if (read != bytesToRead)
				throw new RuntimeException("Could not read full sequence.");

			System.out.println(">" + name);
			System.out.println(new String(data));
		}
		scanner.close();
	}

	private static void testAllEntries(File file, File indexFile) throws IOException {
		BlockCompressedInputStream bcis = new BlockCompressedInputStream(new SeekableFileStream(file));
		bcis.available();

		Scanner fastaScanner = new Scanner(new GZIPInputStream(new FileInputStream(file)));
		Scanner scanner = new Scanner(indexFile);

		int counter = 0;
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] values = line.split("\t");

			String name = values[0];

			int len = Integer.valueOf(values[1]);
			long startPointer = Long.valueOf(values[2]);
			int lineWidthNoNL = Integer.valueOf(values[3]);
			int lineWidthWithNL = Integer.valueOf(values[4]);

			bcis.seek(startPointer);
			int lines = len / lineWidthNoNL + (len % lineWidthNoNL == 0 ? 0 : 1);
			int separatorLen = lineWidthWithNL - lineWidthNoNL;
			int bytesToRead = len + separatorLen * lines - 1;

			byte[] data = new byte[bytesToRead];
			int read = ByteBufferUtils.readFully(data, bcis);
			if (read != bytesToRead)
				throw new RuntimeException("Could not read full sequence.");

			String nameLine = fastaScanner.nextLine();
			if (!nameLine.startsWith(">" + name))
				throw new RuntimeException("Fasta and index entries are not in the same order.");

			byte[] seq = new byte[len];
			ByteBuffer buf = ByteBuffer.wrap(seq);
			for (int i = 0; i < lines; i++) {
				buf.put(fastaScanner.nextLine().getBytes());
			}

			byte[] raSeq = new byte[len];
			int p = 0;
			for (byte b : data) {
				switch (b) {
				case '\r':
				case '\n':
					break;

				default:
					raSeq[p++] = b;
					break;
				}
			}

			if (!Arrays.areEqual(raSeq, seq)) {
				System.out.println(new String(raSeq));
				System.out.println(new String(seq));
				throw new RuntimeException("Sequences are different for entry " + name);
			}

			counter++;
		}
		scanner.close();
		fastaScanner.close();

		System.out.println("Tested " + counter + " sequences.");
	}

	private static File findIndexFile(File fastaFile) {
		File file = new File(fastaFile.getAbsolutePath() + ".fai");
		if (file.exists())
			return file;

		file = new File(fastaFile.getAbsolutePath().replaceAll("\\.gz$", "") + ".fai");
		if (file.exists())
			return file;

		return null;
	}

	@Parameters
	static class Params {
		@Parameter(names = { "-I" }, converter = FileConverter.class)
		File file;

		@Parameter(names = { "--test" })
		boolean test = false;

		@Parameter()
		List<String> names;

	}
}
