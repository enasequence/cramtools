package net.sf.cram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SoftClip;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.structure.SubstitutionMatrix;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;

public class CramNormalizer {
	private SAMFileHeader header;
	private int readCounter = 0;
	private String readNamePrefix = "";
	private byte defaultQualityScore = '?' - '!';

	private static Log log = Log.getInstance(CramNormalizer.class);

	public CramNormalizer(SAMFileHeader header) {
		this.header = header;
	}

	public void normalize(ArrayList<CramRecord> records, boolean resetPairing,
			byte[] ref, int alignmentStart,
			SubstitutionMatrix substitutionMatrix, boolean AP_delta) {

		int startCounter = readCounter;

		for (CramRecord r : records) {
			r.index = ++readCounter;

			if (r.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				r.setSequenceName(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME);
				r.setAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
			} else {
				r.setSequenceName(header.getSequence(r.sequenceId)
						.getSequenceName());
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
					CramRecord downMate = records.get(r.index
							+ r.recordsToNextFragment - startCounter);
					r.next = downMate;
					downMate.previous = r;

					r.mateAlignmentStart = downMate.getAlignmentStart();
					r.mateUmapped = downMate.segmentUnmapped;
					r.mateNegativeStrand = downMate.negativeStrand;
					r.mateSequnceID = downMate.sequenceId;
					if (r.mateSequnceID == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
						r.mateAlignmentStart = SAMRecord.NO_ALIGNMENT_START;

					downMate.mateAlignmentStart = r.getAlignmentStart();
					downMate.mateUmapped = r.segmentUnmapped;
					downMate.mateNegativeStrand = r.negativeStrand;
					downMate.mateSequnceID = r.sequenceId;
					if (downMate.mateSequnceID == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
						downMate.mateAlignmentStart = SAMRecord.NO_ALIGNMENT_START;

					if (r.firstSegment && downMate.lastSegment) {
						r.templateSize = Utils.computeInsertSize(r, downMate);
						downMate.templateSize = -r.templateSize;
					} else if (r.lastSegment && downMate.firstSegment) {
						downMate.templateSize = Utils.computeInsertSize(
								downMate, r);
						r.templateSize = -downMate.templateSize;
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

			byte[] bases = restoreReadBases(r, ref, substitutionMatrix);
			r.setReadBases(bases);
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
			case Deletion.operator:
				len += ((Deletion) rf).getLength();
				break;
			case Insertion.operator:
				len -= ((Insertion) rf).getSequence().length;
				break;
			default:
				break;
			}
		}

		return len;
	}

	private static final byte[] restoreReadBases(CramRecord record, byte[] ref,
			SubstitutionMatrix substitutionMatrix) {
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
			case Substitution.operator:
				Substitution sv = (Substitution) v;
				byte refBase = ref[alignmentStart + posInSeq];
				byte base = substitutionMatrix.base(refBase, sv.getCode());
				// switch (base) {
				// case 'A':
				// case 'C':
				// case 'G':
				// case 'T':
				// case 'N':
				// break;
				//
				// default:
				// throw new RuntimeException("Invalid base: " + base) ;
				// }
				sv.setBase(base);
				sv.setRefernceBase(refBase);
				bases[posInRead++ - 1] = base;
				posInSeq++;
				break;
			case Insertion.operator:
				Insertion iv = (Insertion) v;
				for (int i = 0; i < iv.getSequence().length; i++)
					bases[posInRead++ - 1] = iv.getSequence()[i];
				break;
			case SoftClip.operator:
				SoftClip sc = (SoftClip) v;
				for (int i = 0; i < sc.getSequence().length; i++)
					bases[posInRead++ - 1] = sc.getSequence()[i];
				break;
			case Deletion.operator:
				Deletion dv = (Deletion) v;
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

		for (int i = 0; i < bases.length; i++) {
			switch (bases[i]) {
			case 'a':
				bases[i] = 'A';
				break;
			case 'c':
				bases[i] = 'C';
				break;
			case 'g':
				bases[i] = 'G';
				break;
			case 't':
				bases[i] = 'T';
				break;
			case 'n':
				bases[i] = 'N';
				break;

			default:
				break;
			}
		}

		return bases;
	}
}
