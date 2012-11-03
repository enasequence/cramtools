package net.sf.cram.lossy;

public enum BaseCategoryType {
	MATCH('R'), MISMATCH('N'), FLANKING_DELETION('D'), PILEUP('P'), LOWER_COVERAGE(
			'X');

	public char code;

	BaseCategoryType(char code) {
		this.code = code;
	}
}