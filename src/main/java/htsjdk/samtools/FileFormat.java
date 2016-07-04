package htsjdk.samtools;

import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.BufferedLineReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumSet;

public enum FileFormat {
	UNKNOWN(new byte[0], "", false) {

	},
	SAM("@HD".getBytes(), ".sam", true) {
		@Override
		protected SAMFileHeader readHeader(InputStream is) {
			SAMFileHeader samFileHeader = new SAMTextHeaderCodec().decode(new BufferedLineReader(is), null);
			return samFileHeader;
		}
	},
	BAM(new byte[] { 0x1f, (byte) 0x8b }, ".bam", true) {
		@Override
		protected SAMFileHeader readHeader(InputStream is) throws IOException {
			try {
				BAMFileReader reader = new BAMFileReader(is, null, false, false, ValidationStringency.SILENT, null);
				return reader.getFileHeader();
			} catch (IOException e) {
				throw e;
			} catch (Exception e) {
				return null;
			}
		}
	},
	CRAM("CRAM".getBytes(), ".cram", true) {
		@Override
		protected CramHeader readHeader(InputStream is) throws IOException {
			try {
				return CramIO.readCramHeader(is);
			} catch (IOException e) {
				throw e;
			} catch (Exception ee) {
				return null;
			}
		}
	};
	private static EnumSet<FileFormat> knownFormats = EnumSet.complementOf(EnumSet.of(UNKNOWN));

	private byte[] magic;
	private String fileExtension;
	private boolean doesFormatHasFileHeader;

	private FileFormat(byte[] magic, String fileExtension, boolean doesFormatHasFileHeader) {
		this.magic = magic;
		this.fileExtension = fileExtension;
		this.doesFormatHasFileHeader = doesFormatHasFileHeader;
	}

	private boolean checkMagic(InputStream bis) throws IOException {
		if (magic.length == 0)
			return true;

		bis.mark(magic.length);
		byte[] firstBytes = InputStreamUtils.readFully(bis, magic.length);
		bis.reset();
		return Arrays.equals(magic, firstBytes);
	}

	private boolean checkFileExtension(String source) {
		if (source == null)
			return false;

		if (fileExtension.length() == 0)
			return true;

		return source.toLowerCase().endsWith(fileExtension.toLowerCase());
	}

	public boolean hasHeader() {
		return doesFormatHasFileHeader;
	}

	protected Object readHeader(InputStream is) throws IOException {
		return null;
	};

	public boolean testHeader(InputStream is) throws IOException {
		if (!hasHeader())
			throw new RuntimeException("Format has no concept of file header: " + name());
		if (is != null && is.markSupported()) {
			is.mark(100 * 1024);
			Object header = readHeader(is);
			is.reset();
			return header != null;
		}

		return false;
	}

	public static FileFormat detect(InputStream bis, String source) throws IOException {
		if (bis != null && bis.markSupported()) {
			for (FileFormat format : knownFormats)
				if (format.checkMagic(bis))
					return format;
		}

		if (source != null) {
			for (FileFormat format : knownFormats)
				if (format.checkFileExtension(source))
					return format;
		}

		return UNKNOWN;
	}
}
