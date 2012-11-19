package net.sf.cram;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SoftClipVariation;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;

public class CramNormalizer {
	private SAMFileHeader header;
	private int readCounter = 0;
	private String readNamePrefix = "";
	private int alignmentStart = 1;
	private byte defaultQualityScore = '?' - '!';

	private Map<Integer, CramRecord> pairingByIndexMap = new HashMap<Integer, CramRecord>();
	private byte[] ref;

	public CramNormalizer(SAMFileHeader header, byte[] ref, int alignmentStart) {
		this.header = header;
		this.ref = ref;
		this.alignmentStart = alignmentStart;
	}

	public void normalize(List<CramRecord> records, boolean resetPairing) {
		if (resetPairing)
			pairingByIndexMap.clear();

		for (CramRecord r : records) {
			r.index = ++readCounter;

			alignmentStart += r.alignmentStartOffsetFromPreviousRecord;

			if (r.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				r.setSequenceName(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME);
				r.setAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
			}
			else {
				r.setSequenceName(header.getSequence(r.sequenceId)
						.getSequenceName());
				r.setAlignmentStart(alignmentStart);
			}
		}

		{// restore pairing first:
			for (CramRecord r : records) {
				if (!r.multiFragment || r.detached) {
					r.recordsToNextFragment = -1;

					r.next = null;
					r.previous = null;
					continue;
				}

				if (r.hasMateDownStream) {
					pairingByIndexMap.put(
							r.index + r.recordsToNextFragment + 1, r);
				} else {
					r.recordsToNextFragment = -1;
					CramRecord prev = pairingByIndexMap.remove(r.index);
					if (prev == null)
						throw new RuntimeException("Pairing broken: "
								+ r.toString());
					else {
						r.previous = prev;
						prev.next = r;

						r.mateAlignmentStart = prev.getAlignmentStart();
						r.mateUmapped = prev.segmentUnmapped;
						r.mateNegativeStrand = prev.negativeStrand;
						r.mateSequnceID = prev.sequenceId;
						if (r.mateSequnceID == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
							r.mateAlignmentStart = SAMRecord.NO_ALIGNMENT_START;

						prev.mateAlignmentStart = r.getAlignmentStart();
						prev.mateUmapped = r.segmentUnmapped;
						prev.mateNegativeStrand = r.negativeStrand;
						prev.mateSequnceID = r.sequenceId;
						if (prev.mateSequnceID == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
							prev.mateAlignmentStart = SAMRecord.NO_ALIGNMENT_START;

						if (r.firstSegment && prev.lastSegment) {
							r.templateSize = Utils.computeInsertSize(r, prev);
							prev.templateSize = -r.templateSize;
						} else if (r.lastSegment && prev.firstSegment) {
							prev.templateSize = Utils
									.computeInsertSize(prev, r);
							r.templateSize = -prev.templateSize;
						}
					}
				}
			}
		}

		// assign some read names if needed:
		for (CramRecord r : records) {
			if (r.getReadName() == null) {
				String name = readNamePrefix + r.index;
				r.setReadName(name);
				if (r.next != null)
					r.next.setReadName(name);
				if (r.previous != null)
					r.previous.setReadName(name);
			}
		}

		// resolve bases:
		for (CramRecord r : records) {
			if (r.segmentUnmapped)
				continue;
			byte[] bases = restoreReadBases(r, ref);
			r.setReadBases(bases);
		}

		// restore read group:
		for (CramRecord r : records) {
			r.setReadGroupID(r.getReadGroupID());
		}

		// restore quality scores:
		for (CramRecord r : records) {
			if (!r.forcePreserveQualityScores) {
				byte[] scores = new byte[r.getReadLength()];
				Arrays.fill(scores, defaultQualityScore);
				if (r.getReadFeatures() != null)
					for (ReadFeature f : r.getReadFeatures()) {
						if (f.getOperator() == BaseQualityScore.operator) {
							int pos = f.getPosition();
							byte q = ((BaseQualityScore) f).getQualityScore();

							try {
								scores[pos - 1] = q;
							} catch (ArrayIndexOutOfBoundsException e) {
								System.err.println("PROBLEM CAUSED BY:");
								System.err.println(r.toString());
								throw e;
							}
						}

					}

				r.setQualityScores(scores);
			} else {
				byte[] scores = r.getQualityScores();
				for (int i = 0; i < scores.length; i++)
					if (scores[i] == -1)
						scores[i] = defaultQualityScore;
			}

		}
	}

	public void restoreQualityScores(byte defaultQualityScore,
			List<CramRecord> records) {

	}

	private static final long calcRefLength(CramRecord record) {
		if (record.getReadFeatures() == null
				|| record.getReadFeatures().isEmpty())
			return record.getReadLength();
		long len = record.getReadLength();
		for (ReadFeature rf : record.getReadFeatures()) {
			switch (rf.getOperator()) {
			case DeletionVariation.operator:
				len += ((DeletionVariation) rf).getLength();
				break;
			case InsertionVariation.operator:
				len -= ((InsertionVariation) rf).getSequence().length;
				break;
			default:
				break;
			}
		}

		return len;
	}

	private static final byte[] restoreReadBases(CramRecord record, byte[] ref) {
		int readLength = (int) record.getReadLength();
		byte[] bases = new byte[readLength];

		int posInRead = 1;
		int alignmentStart = record.getAlignmentStart() - 1;

		int posInSeq = 0;
		if (record.getReadFeatures() == null
				|| record.getReadFeatures().isEmpty()) {
			if (ref.length < alignmentStart + bases.length) {
				Arrays.fill(bases, (byte) 'N');
				System.arraycopy(ref, alignmentStart, bases, 0,
						Math.min(bases.length, ref.length - alignmentStart));
			} else
				System.arraycopy(ref, alignmentStart, bases, 0, bases.length);
			return bases;
		}
		List<ReadFeature> variations = record.getReadFeatures();
		for (ReadFeature v : variations) {
			for (; posInRead < v.getPosition(); posInRead++)
				bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

			switch (v.getOperator()) {
			case SubstitutionVariation.operator:
				SubstitutionVariation sv = (SubstitutionVariation) v;
				byte refBase = ref[alignmentStart + posInSeq];
				byte base = sv.getBaseChange().getBaseForReference(refBase);
				sv.setBase(base);
				sv.setRefernceBase(refBase);
				bases[posInRead++ - 1] = sv.getBase();
				posInSeq++;
				break;
			case InsertionVariation.operator:
				InsertionVariation iv = (InsertionVariation) v;
				for (int i = 0; i < iv.getSequence().length; i++)
					bases[posInRead++ - 1] = iv.getSequence()[i];
				break;
			case SoftClipVariation.operator:
				SoftClipVariation sc = (SoftClipVariation) v;
				for (int i = 0; i < sc.getSequence().length; i++)
					bases[posInRead++ - 1] = sc.getSequence()[i];
				break;
			case DeletionVariation.operator:
				DeletionVariation dv = (DeletionVariation) v;
				posInSeq += dv.getLength();
				break;
			case InsertBase.operator:
				InsertBase ib = (InsertBase) v;
				bases[posInRead++ - 1] = ib.getBase();
				break;
			}
		}
		for (; posInRead <= readLength; posInRead++)
			bases[posInRead - 1] = ref[alignmentStart + posInSeq++];

		// ReadBase overwrites bases:
		for (ReadFeature v : variations) {
			switch (v.getOperator()) {
			case ReadBase.operator:
				ReadBase rb = (ReadBase) v;
				bases[v.getPosition() - 1] = rb.getBase();
				break;
			default:
				break;
			}
		}

		return bases;
	}
}
