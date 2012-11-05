package net.sf.cram.lossy;

public enum ReadCategoryType {
	UNPLACED('P'), HIGHER_MAPPING_SCORE('M'), LOWER_MAPPING_SCORE('m');

	public char code;

	ReadCategoryType(char code) {
		this.code = code;
	}
}