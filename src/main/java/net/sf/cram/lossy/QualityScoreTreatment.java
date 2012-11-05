package net.sf.cram.lossy;

public class QualityScoreTreatment {
	public final QualityScoreTreatmentType type;
	public final int param;

	private QualityScoreTreatment(QualityScoreTreatmentType type, int param) {
		this.type = type;
		this.param = param;
	}

	public static QualityScoreTreatment preserve() {
		return new QualityScoreTreatment(
				QualityScoreTreatmentType.PRESERVE, 40);
	}

	public static QualityScoreTreatment drop() {
		return new QualityScoreTreatment(QualityScoreTreatmentType.DROP, 40);
	}

	public static QualityScoreTreatment bin(int bins) {
		return new QualityScoreTreatment(QualityScoreTreatmentType.BIN,
				bins);
	}
	
	@Override
	public String toString() {
		return String.format("[%s%d]", type.name(), param) ;
	}
}