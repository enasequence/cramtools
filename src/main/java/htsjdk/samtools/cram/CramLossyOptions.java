package htsjdk.samtools.cram;

import htsjdk.samtools.cram.lossy.PreservationPolicy;
import htsjdk.samtools.cram.lossy.QualityScorePreservation;
import htsjdk.samtools.cram.lossy.QualityScoreTreatment;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * An object describing lossy nature of CRAM conversion. This is:
 * 
 * <pre>
 * 1. quality score preservation (model)
 * 2. read names
 * 3. capture all tags
 * 4. capture only some tags
 * 5. ignore some tags
 * </pre>
 * 
 * @author vadim
 *
 */
public class CramLossyOptions {
	private QualityScorePreservation preservation = null;
	private boolean preserveReadNames = true;
	private boolean captureAllTags = true;
	private Set<String> captureTags = Collections.emptySet();
	private Set<String> ignoreTags = Collections.emptySet();

	public static final CramLossyOptions lossless() {
		return new CramLossyOptions();
	}

	public boolean isLosslessQualityScore() {
		if (preservation == null)
			return true;

		if (preservation.getPreservationPolicies().isEmpty())
			return false;

		for (PreservationPolicy policy : preservation.getPreservationPolicies()) {
			if (policy.treatment != QualityScoreTreatment.preserve())
				return false;
		}

		return true;
	}

	public boolean areReferenceTracksRequired() {
		return !isLosslessQualityScore() && preservation.areReferenceTracksRequired();
	}

	public CramLossyOptions setPreservation(QualityScorePreservation preservation) {
		this.preservation = preservation;
		return this;
	}

	public CramLossyOptions setPreserveReadNames(boolean preserveReadNames) {
		this.preserveReadNames = preserveReadNames;
		return this;
	}

	public CramLossyOptions setCaptureAllTags(boolean captureAllTags) {
		this.captureAllTags = captureAllTags;
		return this;
	}

	public CramLossyOptions setCaptureTags(Set<String> captureTags) {
		this.captureTags = captureTags;
		return this;
	}

	public CramLossyOptions setCaptureTags(String captureTags) {
		this.captureTags = tagsNamesToSet(captureTags);
		return this;
	}

	public CramLossyOptions setIgnoreTags(Set<String> ignoreTags) {
		this.ignoreTags = ignoreTags;
		return this;
	}

	public CramLossyOptions setIgnoreTags(String ignoreTags) {
		this.ignoreTags = tagsNamesToSet(ignoreTags);
		return this;
	}

	public QualityScorePreservation getPreservation() {
		return preservation;
	}

	public boolean isPreserveReadNames() {
		return preserveReadNames;
	}

	public boolean isCaptureAllTags() {
		return captureAllTags;
	}

	public Set<String> getCaptureTags() {
		return captureTags;
	}

	public Set<String> getIgnoreTags() {
		return ignoreTags;
	}

	public static Set<String> tagsNamesToSet(String tags) {
		Set<String> set = new TreeSet<String>();
		if (tags == null || tags.length() == 0)
			return set;

		String[] chunks = tags.split(":");
		for (String s : chunks) {
			if (s.length() != 2)
				throw new RuntimeException("Expecting column delimited tags names but got: '" + tags + "'");
			set.add(s);
		}
		return set;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("score:").append(preservation.toString());
		sb.append(", ").append("names:").append(preserveReadNames ? "preserve" : "lossy");
		if (captureAllTags) {
			if (ignoreTags == null || ignoreTags.isEmpty()) {
				sb.append(", ").append("tags:all");
			} else {
				sb.append(", tags all but ");
				for (String tag : ignoreTags) {
					sb.append(":").append(tag);
				}
			}
		} else {
			if (captureTags == null || captureTags.isEmpty()) {
				sb.append(", no tags");
			} else {
				sb.append(", capture tags[");
				for (String tag : captureTags) {
					sb.append(":").append(tag);
				}
				sb.append("]");
			}
		}

		return sb.toString();
	}
}