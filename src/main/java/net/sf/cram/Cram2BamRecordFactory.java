package net.sf.cram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SoftClipVariation;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;

public class Cram2BamRecordFactory {

	private SAMFileHeader header;

	public Cram2BamRecordFactory(SAMFileHeader header) {
		this.header = header;
	}

	public SAMRecord create(CramRecord cramRecord) {
		SAMRecord samRecord = new SAMRecord(header);

		samRecord.setReadName(cramRecord.getReadName());
		copyFlags(cramRecord, samRecord);
		samRecord.setReferenceIndex(cramRecord.sequenceId);
		samRecord.setAlignmentStart(cramRecord.getAlignmentStart());
		samRecord.setMappingQuality(cramRecord.getMappingQuality());
		if (cramRecord.segmentUnmapped)
			samRecord.setCigarString(SAMRecord.NO_ALIGNMENT_CIGAR);
		else
			samRecord.setCigar(getCigar2(cramRecord.getReadFeatures(),
					cramRecord.getReadLength()));

		if (samRecord.getReadPairedFlag()) {
			samRecord.setMateReferenceIndex(cramRecord.mateSequnceID);
			samRecord.setMateAlignmentStart(cramRecord.mateAlignmentStart);
			samRecord.setMateNegativeStrandFlag(cramRecord.mateNegativeStrand);
			samRecord.setMateUnmappedFlag(cramRecord.mateUmapped);
		} else {
			samRecord
					.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
			samRecord.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
		}

		samRecord.setInferredInsertSize(cramRecord.templateSize);
		samRecord.setReadBases(cramRecord.getReadBases());
		samRecord.setBaseQualities(cramRecord.getQualityScores());

		if (cramRecord.tags != null)
			for (ReadTag tag : cramRecord.tags)
				samRecord.setAttribute(tag.getKey(), tag.getValue());

		if (cramRecord.getReadGroupID() < header.getReadGroups().size()) {
			SAMReadGroupRecord readGroupRecord = header.getReadGroups().get(
					cramRecord.getReadGroupID());
			samRecord.setAttribute("RG", readGroupRecord.getId());
		}

		return samRecord;
	}

	private static final void copyFlags(CramRecord cr, SAMRecord sr) {
		sr.setReadPairedFlag(cr.multiFragment);
		sr.setProperPairFlag(cr.properPair);
		sr.setReadUnmappedFlag(cr.segmentUnmapped);
		sr.setReadNegativeStrandFlag(cr.negativeStrand);
		sr.setFirstOfPairFlag(cr.firstSegment);
		sr.setSecondOfPairFlag(cr.lastSegment);
		sr.setNotPrimaryAlignmentFlag(cr.secondaryALignment);
		sr.setReadFailsVendorQualityCheckFlag(cr.vendorFiltered);
		sr.setDuplicateReadFlag(cr.duplicate);
	}

	private static final Cigar getCigar2(Collection<ReadFeature> features,
			int readLength) {
		if (features == null || features.isEmpty()) {
			CigarElement ce = new CigarElement(readLength, CigarOperator.M);
			return new Cigar(Arrays.asList(ce));
		}

		List<CigarElement> list = new ArrayList<CigarElement>();
		int totalOpLen = 1;
		CigarElement ce;
		CigarOperator lastOperator = CigarOperator.MATCH_OR_MISMATCH;
		int lastOpLen = 0;
		int lastOpPos = 1;
		CigarOperator co = null;
		int rfLen = 0;
		for (ReadFeature f : features) {

			int gap = f.getPosition() - (lastOpPos + lastOpLen);
			if (gap > 0) {
				if (lastOperator != CigarOperator.MATCH_OR_MISMATCH) {
					list.add(new CigarElement(lastOpLen, lastOperator));
					lastOpPos += lastOpLen;
					totalOpLen += lastOpLen;
					lastOpLen = gap;
				} else {
					lastOpLen += gap;
				}

				lastOperator = CigarOperator.MATCH_OR_MISMATCH;
			}

			switch (f.getOperator()) {
			case InsertionVariation.operator:
				co = CigarOperator.INSERTION;
				rfLen = ((InsertionVariation) f).getSequence().length;
				break;
			case SoftClipVariation.operator:
				co = CigarOperator.SOFT_CLIP;
				rfLen = ((SoftClipVariation) f).getSequence().length;
				break;
			case InsertBase.operator:
				co = CigarOperator.INSERTION;
				rfLen = 1;
				break;
			case DeletionVariation.operator:
				co = CigarOperator.DELETION;
				rfLen = ((DeletionVariation) f).getLength();
				break;
			case SubstitutionVariation.operator:
			case ReadBase.operator:
				co = CigarOperator.MATCH_OR_MISMATCH;
				rfLen = 1;
				break;
			default:
				continue;
			}

			if (lastOperator != co) {
				// add last feature
				if (lastOpLen > 0) {
					list.add(new CigarElement(lastOpLen, lastOperator));
					totalOpLen += lastOpLen;
				}
				lastOperator = co;
				lastOpLen = rfLen;
				lastOpPos = f.getPosition();
			} else
				lastOpLen += rfLen;

			if (co == CigarOperator.DELETION)
				lastOpPos -= rfLen;
		}

		if (lastOperator != null) {
			if (lastOperator != CigarOperator.M) {
				list.add(new CigarElement(lastOpLen, lastOperator));
				if (readLength >= lastOpPos + lastOpLen) {
					ce = new CigarElement(readLength - (lastOpLen + lastOpPos)
							+ 1, CigarOperator.M);
					list.add(ce);
				}
			} else if (readLength > lastOpPos - 1) {
				ce = new CigarElement(readLength - lastOpPos + 1,
						CigarOperator.M);
				list.add(ce);
			}
		}

		if (list.isEmpty()) {
			ce = new CigarElement(readLength, CigarOperator.M);
			return new Cigar(Arrays.asList(ce));
		}

		return new Cigar(list);
	}
}
