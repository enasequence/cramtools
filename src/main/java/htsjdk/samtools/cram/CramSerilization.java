package htsjdk.samtools.cram;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.build.Cram2SamRecordFactory;
import htsjdk.samtools.cram.build.CramNormalizer;
import htsjdk.samtools.cram.build.Sam2CramRecordFactory;
import htsjdk.samtools.cram.ref.ReferenceTracks;
import htsjdk.samtools.cram.structure.AlignmentSpan;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.util.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.ref.ReferenceSource;

/**
 * A static collections of CRAM conversion methods.
 * 
 * @author vadim
 *
 */
public class CramSerilization {
	private static Log log = Log.getInstance(CramSerilization.class);

	public static Container convertLossless(List<SAMRecord> samRecords, SAMFileHeader samFileHeader,
			ReferenceSource source) throws IllegalArgumentException, IllegalAccessException, IOException {
		CramContext context = new CramContext(samFileHeader, source, CramLossyOptions.lossless());
		Map<Integer, AlignmentSpan> spans = getSpans(samRecords);
		return convert(samRecords, context, spans);
	}

	public static Container convert(List<SAMRecord> samRecords, SAMFileHeader samFileHeader, ReferenceSource source,
			CramLossyOptions lossyOptions) throws IllegalArgumentException, IllegalAccessException, IOException {
		CramContext context = new CramContext(samFileHeader, source, lossyOptions);
		Map<Integer, AlignmentSpan> spans = getSpans(samRecords);
		return convert(samRecords, context, spans);
	}

	public static Map<Integer, AlignmentSpan> getSpans(List<SAMRecord> samRecords) {
		Map<Integer, AlignmentSpan> spans = new HashMap<Integer, AlignmentSpan>();
		int unmapped = 0;
		for (final SAMRecord r : samRecords) {

			int refId = r.getReferenceIndex();
			if (refId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				unmapped++;
				continue;
			}

			int start = r.getAlignmentStart();
			int end = r.getAlignmentEnd();

			if (spans.containsKey(refId)) {
				spans.get(refId).add(start, end - start, 1);
			} else {
				spans.put(refId, new AlignmentSpan(start, end - start));
			}
		}
		if (unmapped > 0) {
			AlignmentSpan span = new AlignmentSpan(AlignmentSpan.UNMAPPED_SPAN.getStart(),
					AlignmentSpan.UNMAPPED_SPAN.getSpan(), unmapped);
			spans.put(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX, span);
		}
		return spans;
	}

	public static byte[] getRefBasesOfFail(CramContext cramContext, int refId) {
		if (refId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
			return new byte[0];

		SAMSequenceRecord sequenceRecord = cramContext.samFileHeader.getSequence(refId);
		if (sequenceRecord == null)
			throw new RuntimeException("Reference sequence not found in the header: " + refId);
		byte[] ref = cramContext.referenceSource.getReferenceBases(sequenceRecord, true);
		if (ref == null)
			throw new RuntimeException("Failed to fetch reference bases for sequence " + refId + ", name: "
					+ sequenceRecord.getSequenceName());

		return ref;
	}

	public static Container convert(List<SAMRecord> samRecords, CramContext cramContext)
			throws IllegalArgumentException, IllegalAccessException, IOException {
		return convert(samRecords, cramContext, getSpans(samRecords));
	}

	public static Container convert(List<SAMRecord> samRecords, CramContext cramContext,
			Map<Integer, AlignmentSpan> spans) throws IllegalArgumentException, IllegalAccessException, IOException {

		if (spans.isEmpty())
			throw new IllegalArgumentException("Expecting a valid alignment span or unmapped.");

		if (cramContext.lossyOptions.areReferenceTracksRequired()) {
			for (int refId : spans.keySet()) {
				if (refId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
					continue;

				ReferenceTracks tracks = cramContext.tracks.get(refId);
				if (tracks == null) {
					throw new IllegalArgumentException("The quality score lossy model requires reference tracks.");
				}

				AlignmentSpan span = spans.get(refId);
				tracks.ensureRange(span.getStart(), span.getSpan());
				updateTracks(samRecords, tracks);
			}
		}

		StringBuffer sequences = new StringBuffer();
		for (int refId : spans.keySet()) {
			sequences.append(" ").append(refId);
		}
		log.info(String.format("converting %d records, sequences: %s", samRecords.size(), sequences));

		final List<CramCompressionRecord> cramRecords = new ArrayList<CramCompressionRecord>(samRecords.size());

		final Sam2CramRecordFactory sam2CramRecordFactory = cramContext.sam2cramFactory;

		int index = 0;
		int refId = Integer.MIN_VALUE;
		byte[] ref = null;

		for (SAMRecord samRecord : samRecords) {
			if (samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				// update ref in the factory:
				if (refId != samRecord.getReferenceIndex()) {
					refId = samRecord.getReferenceIndex();
					ref = getRefBasesOfFail(cramContext, refId);
					sam2CramRecordFactory.setRefBases(ref);
				}
			}

			final CramCompressionRecord cramRecord = sam2CramRecordFactory.createCramRecord(samRecord);
			cramRecord.index = ++index;
			cramRecord.alignmentStart = samRecord.getAlignmentStart();

			cramRecords.add(cramRecord);

			if (cramContext.lossyOptions.isLosslessQualityScore())
				cramRecord.setForcePreserveQualityScores(cramRecord.qualityScores != SAMRecord.NULL_QUALS);
			else {
				ReferenceTracks tracks = cramContext.tracks.get(samRecord.getReferenceIndex());
				cramContext.lossyOptions.getPreservation().addQualityScores(samRecord, cramRecord, tracks);
			}
		}

		if (sam2CramRecordFactory.getBaseCount() < 3 * sam2CramRecordFactory.getFeatureCount()) {
			log.warn("Abnormally high number of mismatches, possibly wrong reference.");
		}

		if (cramContext.samFileHeader.getSortOrder() == SAMFileHeader.SortOrder.coordinate) {
			setMateInfo(cramRecords);
		} else {
			for (final CramCompressionRecord cramRecord : cramRecords) {
				cramRecord.setDetached(true);
			}
		}

		assertSame(cramContext.samFileHeader, samRecords, cramRecords);

		final Container container = cramContext.containerFactory.buildContainer(cramRecords);

		for (final Slice slice : container.slices) {
			if (slice.alignmentSpan < 0)
				slice.alignmentSpan = 0;
			switch (slice.sequenceId) {
			case Slice.MULTI_REFERENCE:
				continue;
			case SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX:
				slice.setRefMD5(new byte[0]);
				continue;
			default:
				slice.setRefMD5(getRefBasesOfFail(cramContext, slice.sequenceId));
				break;
			}
		}

		return container;
	}

	/**
	 * The following passage is for paranoid mode only. When java is run with
	 * asserts on it will throw an {@link AssertionError} if read bases or
	 * quality scores of a restored SAM record mismatch the original. This is
	 * effectively a runtime round trip test.
	 */
	private static void assertSame(SAMFileHeader samFileHeader, List<SAMRecord> samRecords,
			List<CramCompressionRecord> cramRecords) {
		@SuppressWarnings("UnusedAssignment")
		boolean assertsEnabled = false;
		assert assertsEnabled = true;
		if (assertsEnabled) {
			final Cram2SamRecordFactory f = new Cram2SamRecordFactory(samFileHeader);
			for (int i = 0; i < samRecords.size(); i++) {
				final SAMRecord restoredSamRecord = f.create(cramRecords.get(i));
				assert (restoredSamRecord.getAlignmentStart() == samRecords.get(i).getAlignmentStart());
				assert (restoredSamRecord.getReferenceName().equals(samRecords.get(i).getReferenceName()));
				assert (restoredSamRecord.getReadString().equals(samRecords.get(i).getReadString()));
				assert (restoredSamRecord.getBaseQualityString().equals(samRecords.get(i).getBaseQualityString()));
			}
		}
	}

	private static void updateTracks(final List<SAMRecord> samRecords, final ReferenceTracks tracks) {
		for (final SAMRecord samRecord : samRecords) {
			if (samRecord.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START
					&& samRecord.getReferenceIndex() == tracks.getSequenceId()) {
				int refPos = samRecord.getAlignmentStart();
				int readPos = 0;
				for (final CigarElement cigarElement : samRecord.getCigar().getCigarElements()) {
					if (cigarElement.getOperator().consumesReferenceBases()) {
						for (int elementIndex = 0; elementIndex < cigarElement.getLength(); elementIndex++)
							tracks.addCoverage(refPos + elementIndex, 1);
					}
					switch (cigarElement.getOperator()) {
					case M:
					case X:
					case EQ:
						for (int pos = readPos; pos < cigarElement.getLength(); pos++) {
							final byte readBase = samRecord.getReadBases()[readPos + pos];
							final byte refBase = tracks.baseAt(refPos + pos);
							if (readBase != refBase)
								tracks.addMismatches(refPos + pos, 1);
						}
						break;

					default:
						break;
					}

					readPos += cigarElement.getOperator().consumesReadBases() ? cigarElement.getLength() : 0;
					refPos += cigarElement.getOperator().consumesReferenceBases() ? cigarElement.getLength() : 0;
				}
			}
		}
	}

	/**
	 * Traverse the graph and mark all segments as detached.
	 *
	 * @param cramRecord
	 *            the starting point of the graph
	 */
	private static void detach(CramCompressionRecord cramRecord) {
		do {
			cramRecord.setDetached(true);

			cramRecord.setHasMateDownStream(false);
			cramRecord.recordsToNextFragment = -1;
		} while ((cramRecord = cramRecord.next) != null);
	}

	private static List<CramCompressionRecord> setMateInfo(List<CramCompressionRecord> cramRecords) {
		Map<String, CramCompressionRecord> primaryMateMap = new TreeMap<String, CramCompressionRecord>();
		Map<String, CramCompressionRecord> secondaryMateMap = new TreeMap<String, CramCompressionRecord>();
		for (CramCompressionRecord r : cramRecords) {
			if (!r.isMultiFragment()) {
				r.setDetached(true);

				r.setHasMateDownStream(false);
				r.recordsToNextFragment = -1;
				r.next = null;
				r.previous = null;
			} else {
				String name = r.readName;
				Map<String, CramCompressionRecord> mateMap = r.isSecondaryAlignment() ? secondaryMateMap
						: primaryMateMap;
				CramCompressionRecord mate = mateMap.get(name);
				if (mate == null) {
					mateMap.put(name, r);
				} else {
					CramCompressionRecord prev = mate;
					if (prev.next != null || prev.previous != null) {
						detach(r);
						detach(prev);
						continue;
					}
					while (prev.next != null)
						prev = prev.next;
					prev.recordsToNextFragment = r.index - prev.index - 1;
					prev.next = r;
					r.previous = prev;
					r.previous.setHasMateDownStream(true);
					r.setHasMateDownStream(false);
					r.setDetached(false);
					r.previous.setDetached(false);
				}
			}
		}

		// mark unpredictable reads as detached:
		for (CramCompressionRecord r : cramRecords) {
			if (r.next == null || r.previous != null)
				continue;
			CramCompressionRecord last = r;
			while (last.next != null)
				last = last.next;

			if ((r.isFirstSegment() && last.isLastSegment()) || (last.isFirstSegment() && r.isLastSegment())) {

				final int templateLength = CramNormalizer.computeInsertSize(r, last);
				if (r.templateSize == templateLength) {
					last = r.next;
					while (last.next != null) {
						if (last.templateSize != -templateLength)
							break;

						last = last.next;
					}
					if (last.templateSize != -templateLength) {
						detach(r);
					}
				} else {
					detach(r);
				}
			} else {
				detach(r);
			}

			if (r.mateSequenceID != last.sequenceId || r.sequenceId != last.mateSequenceID
					|| (r.isMateNegativeStrand() != last.isNegativeStrand())
					|| (last.isMateNegativeStrand() != r.isNegativeStrand())
					|| (r.mateAlignmentStart != last.alignmentStart) || (last.mateAlignmentStart != r.alignmentStart)) {
				detach(r);
			}

		}

		for (CramCompressionRecord r : primaryMateMap.values()) {
			if (r.next != null)
				continue;
			r.setDetached(true);

			r.setHasMateDownStream(false);
			r.recordsToNextFragment = -1;
			r.next = null;
			r.previous = null;
		}

		for (CramCompressionRecord r : secondaryMateMap.values()) {
			if (r.next != null)
				continue;
			r.setDetached(true);

			r.setHasMateDownStream(false);
			r.recordsToNextFragment = -1;
			r.next = null;
			r.previous = null;
		}

		return cramRecords;
	}
}
