package htsjdk.samtools;

import java.io.File;
import java.util.EnumSet;

public enum IndexFormat {
	UNKNOWN, BAI, CRAI;
	private static EnumSet<IndexFormat> knownFormats = EnumSet.complementOf(EnumSet.of(UNKNOWN));

	public static IndexFormat fromString(String string) {
		for (IndexFormat format : knownFormats) {
			if (format.name().equalsIgnoreCase(string))
				return format;
		}
		return UNKNOWN;
	}

	public File getDefaultIndexFileNameForDataFile(File dataFile) {
		if (this == UNKNOWN)
			return null;

		return new File(dataFile.getAbsolutePath() + "." + name().toLowerCase());
	}
}
