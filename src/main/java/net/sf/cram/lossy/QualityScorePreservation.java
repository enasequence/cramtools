package net.sf.cram.lossy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import net.sf.cram.CramRecord;
import net.sf.cram.ReferenceTracks;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;

public class QualityScorePreservation {
	private String specification;
	private List<PreservationPolicy> policyList;

	public QualityScorePreservation(String specification) {
		this.specification = specification;
		policyList = new ArrayList<PreservationPolicy>();
		for (String s : specification.split("-")) {
			if (s.length() == 0)
				continue;
			PreservationPolicy policy = parseSinglePolicy(s);
			// System.err.println("Adding preservation policy: "
			// + policy.toString());
			policyList.add(policy);
		}

		Collections.sort(policyList, new Comparator<PreservationPolicy>() {

			@Override
			public int compare(PreservationPolicy o1, PreservationPolicy o2) {
				QualityScoreTreatment t1 = o1.treatment;
				QualityScoreTreatment t2 = o2.treatment;
				int result = t2.type.ordinal() - t1.type.ordinal();
				if (result != 0)
					return result;

				return 0;
			}
		});
	}
	
	public List<PreservationPolicy> getPreservationPolicies () {
		return policyList ;
	}

	private static final int readParam(LinkedList<Character> list) {
		int value = 0;

		while (!list.isEmpty() && Character.isDigit(list.getFirst())) 
			value = value * 10 + (list.removeFirst() - 48);

		return value;
	}

	private static final QualityScoreTreatment readTreament(
			LinkedList<Character> list) {
		int param = readParam(list);
		QualityScoreTreatment t;
		switch (param) {
		case 0:
			t = QualityScoreTreatment.drop();
			break;
		case 40:
			t = QualityScoreTreatment.preserve();
			break;

		default:
			t = QualityScoreTreatment.bin(param);
			break;

		}
		return t;
	}

	private static final PreservationPolicy parseSinglePolicy(String spec) {
		PreservationPolicy p = new PreservationPolicy();
		LinkedList<Character> list = new LinkedList<Character>();
		for (char b : spec.toCharArray())
			list.add(b);

		while (!list.isEmpty()) {
			char code = list.removeFirst();
			switch (code) {
			case 'R':
				p.baseCategories.add(BaseCategory.match());
				p.treatment = readTreament(list);
				break;
			case 'N':
				p.baseCategories.add(BaseCategory.mismatch());
				p.treatment = readTreament(list);
				break;
			case 'X':
				int coverage = readParam(list);
				p.baseCategories
						.add(BaseCategory.lower_than_coverage(coverage));
				break;
			case 'D':
				p.baseCategories.add(BaseCategory.flanking_deletion());
				p.treatment = readTreament(list);
				break;
			case 'M':
				int score = readParam(list);
				p.readCategory = ReadCategory.higher_than_mapping_score(score);
				break;
			case 'm':
				score = readParam(list);
				p.readCategory = ReadCategory.lower_than_mapping_score(score);
				break;
			case 'U':
				p.readCategory = ReadCategory.unplaced();
				p.treatment = readTreament(list);
				break;
			case 'P':
				int mismatches = readParam(list) ;
				p.baseCategories.add(BaseCategory.pileup(mismatches));
				p.treatment = readTreament(list);
				break;
			case 'I':
				p.baseCategories.add(BaseCategory.insertion());
				p.treatment = readTreament(list);
				break;
//			case '_':
//				p.treatment = readTreament(list);
//				break;
//			case '*':
//				p.readCategory = ReadCategory.all();
//				p.treatment = readTreament(list);
//				break;

			default:
				throw new RuntimeException("Uknown read or base category: "
						+ code);
			}

			if (p.treatment == null)
				p.treatment = QualityScoreTreatment.preserve();
		}

		return p;
	}

	// // public static List<PreservationPolicy> policyList = new ArrayList<>();
	// static {
	// // these should be sorted by qs treatment from none to max!
	//
	// // M40
	// PreservationPolicy c1 = new PreservationPolicy();
	// c1.readCategory = ReadCategory.higher_then_mapping_score(0) ;
	// c1.treatment = QualityScoreTreatment.preserve();
	// policyList.add(c1);
	//
	// // // N8
	// // PreservationPolicy c1 = new PreservationPolicy();
	// // c1.baseCategories.add(BaseCategory.mismatch());
	// // c1.treatment = QualityScoreTreatment.preserve();
	// // policyList.add(c1);
	//
	// // // R8X10-R40X5-N40-U40
	// // PreservationPolicy c1 = new PreservationPolicy();
	// // c1.baseCategories.add(BaseCategory.lower_than_coverage(10));
	// // c1.baseCategories.add(BaseCategory.match());
	// // c1.treatment = QualityScoreTreatment.bin(8);
	// // policyList.add(c1);
	// //
	// // PreservationPolicy c2 = new PreservationPolicy();
	// // c1.baseCategories.add(BaseCategory.lower_than_coverage(5));
	// // c1.baseCategories.add(BaseCategory.match());
	// // c2.treatment = QualityScoreTreatment.bin(40);
	// // policyList.add(c2);
	// //
	// // PreservationPolicy c3 = new PreservationPolicy();
	// // c1.baseCategories.add(BaseCategory.mismatch());
	// // c3.treatment = QualityScoreTreatment.bin(40);
	// // policyList.add(c3);
	// //
	// // PreservationPolicy c4 = new PreservationPolicy();
	// // c4.readCategory = ReadCategory.unplaced();
	// // c4.treatment = QualityScoreTreatment.bin(40);
	// // policyList.add(c4);
	// }
	// static {
	// Collections.sort(policyList, new Comparator<PreservationPolicy>() {
	//
	// @Override
	// public int compare(PreservationPolicy o1, PreservationPolicy o2) {
	// QualityScoreTreatment t1 = o1.treatment;
	// QualityScoreTreatment t2 = o2.treatment;
	// int result = t2.type.ordinal() - t1.type.ordinal();
	// if (result != 0)
	// return result;
	//
	// return 0;
	// }
	// });
	// }

	private static final void applyBinning(byte[] scores) {
		for (int i = 0; i < scores.length; i++)
			scores[i] = Binning.Illumina_binning_matrix[scores[i]];
	}

	private static final byte applyTreatment(byte score, QualityScoreTreatment t) {
		switch (t.type) {
		case BIN:
			return (byte) (Binning.Illumina_binning_matrix[score]);
		case DROP:
			return -1;
		case PRESERVE:
			return score;

		}
		throw new RuntimeException("Unknown quality score treatment type: "
				+ t.type.name());
	}

	public void addQualityScores(SAMRecord s, CramRecord r, ReferenceTracks t) {
		byte[] scores = new byte[s.getReadLength()];
		Arrays.fill(scores, (byte) -1);
		for (PreservationPolicy p : policyList)
			addQS(s, r, scores, t, p);

		if (!r.forcePreserveQualityScores) {
			for (int i = 0; i < scores.length; i++) {
				if (scores[i] > -1) {
					if (r.getReadFeatures() == null) r.setReadFeatures(new LinkedList<ReadFeature>()) ;
					r.getReadFeatures().add(
							new BaseQualityScore(i + 1, scores[i]));
				}
			}
			if (r.getReadFeatures() != null)
				Collections.sort(r.getReadFeatures(),
						readFeaturePositionComparator);
		} else
			r.setQualityScores(scores);
	}

	private static final Comparator<ReadFeature> readFeaturePositionComparator = new Comparator<ReadFeature>() {

		@Override
		public int compare(ReadFeature o1, ReadFeature o2) {
			return o1.getPosition() - o2.getPosition();
		}
	};

	private static final void addQS(SAMRecord s, CramRecord r, byte[] scores,
			ReferenceTracks t, PreservationPolicy p) {
		int alSpan = s.getAlignmentEnd() - s.getAlignmentStart();
		byte[] qs = s.getBaseQualities();

		// check if read is falling into the read category:
		if (p.readCategory != null) {
			boolean properRead = false;
			switch (p.readCategory.type) {
			case UNPLACED:
				properRead = s.getReadUnmappedFlag();
				break;
			case LOWER_MAPPING_SCORE:
				properRead = s.getMappingQuality() < p.readCategory.param;
				break;
			case HIGHER_MAPPING_SCORE:
				properRead = s.getMappingQuality() > p.readCategory.param;
				break;

			default:
				throw new RuntimeException("Unknown read category: "
						+ p.readCategory.type.name());
			}

			if (!properRead) // nothing to do here:
				return;
		}

		// apply treamtent if there is no per-base policy:
		if (p.baseCategories == null || p.baseCategories.isEmpty()) {
			switch (p.treatment.type) {
			case BIN:
				if (r.getQualityScores() == null)
					r.setQualityScores(s.getBaseQualities());
				System.arraycopy(s.getBaseQualities(), 0, scores, 0,
						scores.length);
				applyBinning(scores);
				r.forcePreserveQualityScores = true;
				break;
			case PRESERVE:
				System.arraycopy(s.getBaseQualities(), 0, scores, 0,
						scores.length);
				r.forcePreserveQualityScores = true;
				break;
			case DROP:
				r.setReadBases(null);
				r.forcePreserveQualityScores = false;
				break;

			default:
				throw new RuntimeException(
						"Unknown quality score treatment type: "
								+ p.treatment.type.name());
			}

			// nothing else to do here:
			return;
		}

		// here we go, scan all bases to check if the policy applies:
		boolean[] mask = new boolean[qs.length];
		int alStart = s.getAlignmentStart();
		// must be a mapped read at this point:
		if (alStart == SAMRecord.NO_ALIGNMENT_START)
			return;
		t.ensureRange(alStart, alSpan);

		for (BaseCategory c : p.baseCategories) {
			int pos;
			int refPos;
			switch (c.type) {
			case FLANKING_DELETION:
				pos = 0;
				for (CigarElement ce : s.getCigar().getCigarElements()) {
					if (ce.getOperator() == CigarOperator.D) {
						// if (pos > 0)
						mask[pos] = true;
						if (pos + 1 < mask.length)
							mask[pos + 1] = true;
					}

					pos += ce.getOperator().consumesReadBases() ? ce
							.getLength() : 0;
				}
				break;
			case MATCH:
			case MISMATCH:
				pos = 0;
				refPos = s.getAlignmentStart();
				for (CigarElement ce : s.getCigar().getCigarElements()) {
					switch (ce.getOperator()) {
					case M:
					case X:
					case EQ:
						for (int i = 0; i < ce.getLength(); i++) {
							boolean match = s.getReadBases()[pos + i] == t
									.baseAt(refPos + i);
							if ((c.type == BaseCategoryType.MATCH && match)
									|| (c.type == BaseCategoryType.MISMATCH && !match)) {
								mask[pos + i] = true;
							}
						}
						break;
					default:
						break;
					}

					pos += ce.getOperator().consumesReadBases() ? ce
							.getLength() : 0;
					refPos += ce.getOperator().consumesReferenceBases() ? ce
							.getLength() : 0;
				}
				break;
			case INSERTION:
				pos = 0;
				for (CigarElement ce : s.getCigar().getCigarElements()) {
					switch (ce.getOperator()) {
					case I:
						for (int i = 0; i < ce.getLength(); i++)
							mask[pos + i] = true;
						break;
					default:
						break;
					}

					pos += ce.getOperator().consumesReadBases() ? ce
							.getLength() : 0;
				}
				break;
			case LOWER_COVERAGE:
				for (int i = 0; i < qs.length; i++)
					if (t.coverageAt(alStart + i) < c.param)
						mask[i] = true;
				break;
			case PILEUP:
				for (int i = 0; i < qs.length; i++)
					if (t.mismatchesAt(alStart + i) > c.param)
						mask[i] = true;
				break;

			default:
				break;
			}

			int maskedCount = 0;
			for (int i = 0; i < mask.length; i++)
				if (mask[i]) {
					scores[i] = applyTreatment(qs[i], p.treatment);
					maskedCount++;
				}
			// safety latch, store all qs if there are too many individual score
			// to store:
			if (maskedCount > s.getReadLength() / 2)
				r.forcePreserveQualityScores = true;
		}
	}
}
