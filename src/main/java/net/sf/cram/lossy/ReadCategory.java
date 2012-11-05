package net.sf.cram.lossy;

public class ReadCategory {
	public final ReadCategoryType type;
	public final int param;

	private ReadCategory(ReadCategoryType type, int param) {
		this.type = type;
		this.param = param;
	}

	public static ReadCategory unplaced() {
		return new ReadCategory(ReadCategoryType.UNPLACED, -1);
	};

	public static ReadCategory higher_than_mapping_score(int score) {
		return new ReadCategory(ReadCategoryType.HIGHER_MAPPING_SCORE,
				score);
	};

	public static ReadCategory lower_than_mapping_score(int score) {
		return new ReadCategory(ReadCategoryType.LOWER_MAPPING_SCORE, score);
	};
	
	@Override
	public String toString() {
		return String.format("[%s%d]", type.name(), param) ;
	}
}