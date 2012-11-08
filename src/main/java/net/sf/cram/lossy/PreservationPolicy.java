package net.sf.cram.lossy;

import java.util.ArrayList;
import java.util.List;

public class PreservationPolicy {
	public ReadCategory readCategory;
	public List<BaseCategory> baseCategories = new ArrayList<BaseCategory>();

	public QualityScoreTreatment treatment;

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if (readCategory != null)
			sb.append(readCategory.toString());

		if (baseCategories != null)
			for (BaseCategory c : baseCategories)
				sb.append(c.toString());

		sb.append(treatment.toString());
		return sb.toString();
	}
}