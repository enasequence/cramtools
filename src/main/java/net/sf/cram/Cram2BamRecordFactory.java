package net.sf.cram;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;

public class Cram2BamRecordFactory {

	private SAMFileHeader header;
	private int counter;

	public Cram2BamRecordFactory(SAMFileHeader header) {
		this.header = header;
	}

	public SAMRecord create(CramRecord cramRecord) {
		SAMRecord samRecord = new SAMRecord(header);

		samRecord.setReadName(cramRecord.getReadName());
		samRecord.setFlags(cramRecord.getSamFlags());
		samRecord.setReferenceIndex(cramRecord.sequenceId);
		samRecord.setAlignmentStart(cramRecord.getAlignmentStart());
		samRecord.setMappingQuality(cramRecord.getMappingQuality());
		samRecord.setCigar(cramRecord.getCigar());

		if (samRecord.getReadPairedFlag()) {
			samRecord.setMateReferenceIndex(cramRecord.mateSequnceID);
			samRecord.setMateAlignmentStart(cramRecord.mateAlignmentStart);
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
				samRecord.getAttributes().add(tag.createSAMTag());

		return samRecord;
	}
}
