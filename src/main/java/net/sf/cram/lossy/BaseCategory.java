package net.sf.cram.lossy;

public class BaseCategory {
	public final BaseCategoryType type;
	public final int param;

	private BaseCategory(BaseCategoryType type, int param) {
		this.type = type;
		this.param = param;
	}

	public static BaseCategory match() {
		return new BaseCategory(BaseCategoryType.MATCH, -1);
	}

	public static BaseCategory mismatch() {
		return new BaseCategory(BaseCategoryType.MISMATCH, -1);
	}

	public static BaseCategory flanking_deletion() {
		return new BaseCategory(BaseCategoryType.FLANKING_DELETION, -1);
	}

	public static BaseCategory pileup(int threshold) {
		return new BaseCategory(BaseCategoryType.PILEUP, threshold);
	}

	public static BaseCategory lower_than_coverage(int coverage) {
		return new BaseCategory(BaseCategoryType.LOWER_COVERAGE, coverage);
	};
	
	@Override
	public String toString() {
		return String.format("[%s%d]", type.name(), param) ;
	}
}