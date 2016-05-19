package net.sf.cram;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.CramTools.LevelConverter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class DownloadReferences {
	private static Log log = Log.getInstance(DownloadReferences.class);
	public static final String COMMAND = "getref";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + DownloadReferences.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		List<SeqID> idList = new ArrayList<SeqID>();
		if (params.md5List != null) {
			for (String id : params.md5List)
				idList.add(new SeqID(id));
		}

		if (params.cramFile == null) {
			log.error("Expecting a file.");
			System.exit(1);
		}

		if (!params.cramFile.exists()) {
			log.error("File does not exist: " + params.cramFile.getAbsolutePath());
			System.exit(1);
		}
		if (!params.cramFile.canRead()) {
			log.error("Cannot read file: " + params.cramFile.getAbsolutePath());
			System.exit(1);
		}

		SAMFileHeader samFileHeader = null;
		try {
			CramHeader cramHeader = CramIO.readCramHeader(new FileInputStream(params.cramFile));
			samFileHeader = cramHeader.getSamFileHeader();
		} catch (Exception e) {
			// try to open using standard way, perhaps this is a valid file
			// but not in CRAM format:
			log.info("Not a cram file, trying other formats...");
			try {
				SamReader samReader = SamReaderFactory.make().open(params.cramFile);
				samFileHeader = samReader.getFileHeader();
			} catch (Exception e1) {
				log.error("Failed to open file: unknown file format.");
				System.exit(1);
			}
		}

		for (SAMSequenceRecord s : samFileHeader.getSequenceDictionary().getSequences()) {
			String md5 = s.getAttribute(SAMSequenceRecord.MD5_TAG);
			if (md5 != null)
				idList.add(new SeqID(s.getSequenceName(), md5));
		}
		if (idList.size() < samFileHeader.getSequenceDictionary().size()) {
			log.warn(String.format("File header contains %d sequences but only %d have md5 checksums.", samFileHeader
					.getSequenceDictionary().size(), idList.size()));
		}

		OutputStream os = (params.destFile != null ? os = openOptionallyGzippedFile(params.destFile.getAbsolutePath(),
				params.gzip) : System.out);

		downloadSequences(os, idList, params.ignoreNotFound, params.lineLength);

		if (os != System.out)
			os.close();
	}

	private static class SeqID {
		private static final int MD5_LEN = 32;
		String name;
		String md5;

		/**
		 * ID is of form "name:md5".
		 * 
		 * @param id
		 */
		public SeqID(String id) {
			md5 = id.substring(id.length() - MD5_LEN);
			name = id;
			if (id.length() > MD5_LEN + 1)
				name = id.substring(0, id.length() - MD5_LEN - 1);
			if (name == null || name.length() == 0)
				name = md5;
		}

		public SeqID(String name, String md5) {
			this.name = name;
			this.md5 = md5;
		}
	}

	private static void downloadSequences(OutputStream bos, List<SeqID> idList, boolean ignoreNotFound, int lineLength) {
		try {
			for (SeqID id : idList) {
				log.info(String.format("Locating sequence %s for MD5 %s", id.name, id.md5));
				InputStream is = null;
				try {
					is = getInputStreamForMD5(id.md5);
				} catch (IOException ioe) {
					String message = ioe.getMessage();
					if (message != null & message.startsWith("Server returned HTTP response code: 500")) {
						if (ignoreNotFound) {
							log.warn(String.format("Not found in the remote repository: sequence '%s' for MD5 %s",
									id.name, id.md5));
							continue;
						} else {
							log.error(String.format("Not found in the remote repository: sequence '%s' for MD5 %s",
									id.name, id.md5));
							throw ioe;
						}
					} else
						throw ioe;

				}

				printSequence(is, id, bos, lineLength);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void printSequence(InputStream is, SeqID id, OutputStream bos, int lineLength) throws IOException {
		bos.write('>');
		bos.write(id.name.getBytes());
		bos.write(' ');
		bos.write(id.md5.getBytes());
		bos.write('\n');
		copy(is, bos, lineLength);
	}

	private static OutputStream openOptionallyGzippedFile(String name, boolean gzip) throws IOException {
		FileOutputStream fos = new FileOutputStream(name + (gzip ? ".gz" : ""));
		OutputStream bos = new BufferedOutputStream(fos);
		if (gzip)
			bos = new GZIPOutputStream(bos);
		return bos;
	}

	private static void copy(InputStream in, OutputStream out, int lineLength) throws IOException {
		int count;
		byte[] buffer = new byte[8192];
		int posInLine = 0, posInBuf = 0;
		while ((count = in.read(buffer)) > -1) {
			posInBuf = 0;
			while (posInBuf < count) {
				for (; posInLine < lineLength && posInBuf < count; posInLine++) {
					out.write(buffer[posInBuf++]);
				}
				if (posInLine >= lineLength) {
					posInLine = 0;
					out.write('\n');
				}
			}
		}
		if (posInLine > 0)
			out.write('\n');
	}

	private static InputStream getInputStreamForMD5(String md5) throws IOException {
		String urlString = String.format("http://www.ebi.ac.uk/ena/cram/md5/%s", md5);
		URL url = new URL(urlString);
		return url.openStream();
	}

	@Parameters(commandDescription = "Download reference sequences.")
	static class Params {

		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM or BAM file to extract sequence MD5 checksums.")
		File cramFile;

		@Parameter(names = { "--gzip", "-z" }, description = "Compress fasta with gzip.")
		boolean gzip;

		@Parameter(description = "A list of MD5 checksums for which the sequences should be downloaded.")
		List<String> md5List;

		@Parameter(names = { "--destination-file", "-F" }, description = "Destination file.")
		File destFile;

		@Parameter(names = { "--ignore-not-found" }, description = "Don't fail on not found sequences, just issue a warning.")
		boolean ignoreNotFound = false;

		@Parameter(names = { "--fasta-line-length" }, description = "Wrap fasta lines accroding to this value.")
		public int lineLength = 80;
	}
}
