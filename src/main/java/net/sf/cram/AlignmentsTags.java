package net.sf.cram;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTag;
import htsjdk.samtools.util.SequenceUtil;

import java.io.IOException;
import java.util.List;

import net.sf.cram.ref.ReferenceRegion;
import net.sf.cram.ref.ReferenceSource;

public class AlignmentsTags {

	/**
	 * Convenience method. See {@link
	 * net.sf.cram.AlignmentsTags.calculateMdAndNmTags(SAMRecord, byte[], int,
	 * boolean, boolean)} for details.
	 * 
	 * @param samRecord
	 * @param referenceSource
	 * @param samSequenceDictionary
	 * @param calculateMdTag
	 * @param calculateNmTag
	 * @throws IOException
	 */
	public static void calculateMdAndNmTags(SAMRecord samRecord, ReferenceSource referenceSource,
			SAMSequenceDictionary samSequenceDictionary, boolean calculateMdTag, boolean calculateNmTag)
			throws IOException {
		if (!samRecord.getReadUnmappedFlag() && samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			SAMSequenceRecord sequence = samSequenceDictionary.getSequence(samRecord.getReferenceIndex());
			ReferenceRegion region = referenceSource.getRegion(sequence, samRecord.getAlignmentStart(),
					samRecord.getAlignmentEnd());
			calculateMdAndNmTags(samRecord, region.array, samRecord.getAlignmentStart() - 1, calculateMdTag,
					calculateNmTag);
		}
	}

	public static void calculateMdAndNmTags(final SAMRecord record, final byte[] ref, final boolean calcMD,
			final boolean calcNM) {
		SequenceUtil.calculateMdAndNmTags(record, ref, calcMD, calcNM);
	}

	/**
	 * Same as {@link htsjdk.samtools.util.SequenceUtil#calculateMdAndNmTags}
	 * but allows for reference excerpt instead of a full reference sequence.
	 * 
	 * @param record
	 *            SAMRecord to inject NM and MD tags into
	 * @param ref
	 *            reference sequence bases, may be a subsequence starting at
	 *            refOffest
	 * @param refOffset
	 *            array offset of the reference bases, 0 is the same as the
	 *            whole sequence. This value will be subtracted from the
	 *            reference array index in calculations.
	 * @param calcMD
	 *            calculate MD tag if true
	 * @param calcNM
	 *            calculate NM tag if true
	 */
	public static void calculateMdAndNmTags(final SAMRecord record, final byte[] ref, final int refOffset,
			final boolean calcMD, final boolean calcNM) {
		if (!calcMD && !calcNM)
			return;

		final Cigar cigar = record.getCigar();
		final List<CigarElement> cigarElements = cigar.getCigarElements();
		final byte[] seq = record.getReadBases();
		final int start = record.getAlignmentStart() - 1;
		int i, x, y, u = 0;
		int nm = 0;
		final StringBuilder str = new StringBuilder();

		final int size = cigarElements.size();
		for (i = y = 0, x = start; i < size; ++i) {
			final CigarElement ce = cigarElements.get(i);
			int j;
			final int length = ce.getLength();
			final CigarOperator op = ce.getOperator();
			if (op == CigarOperator.MATCH_OR_MISMATCH || op == CigarOperator.EQ || op == CigarOperator.X) {
				for (j = 0; j < length; ++j) {
					final int z = y + j;

					if (refOffset + ref.length <= x + j)
						break; // out of boundary

					int c1 = 0;
					int c2 = 0;
					// try {
					c1 = seq[z];
					c2 = ref[x + j - refOffset];

					if ((c1 == c2) || c1 == 0) {
						// a match
						++u;
					} else {
						str.append(u);
						str.appendCodePoint(ref[x + j - refOffset]);
						u = 0;
						++nm;
					}
				}
				if (j < length)
					break;
				x += length;
				y += length;
			} else if (op == CigarOperator.DELETION) {
				str.append(u);
				str.append('^');
				for (j = 0; j < length; ++j) {
					if (ref[x + j - refOffset] == 0)
						break;
					str.appendCodePoint(ref[x + j - refOffset]);
				}
				u = 0;
				if (j < length)
					break;
				x += length;
				nm += length;
			} else if (op == CigarOperator.INSERTION || op == CigarOperator.SOFT_CLIP) {
				y += length;
				if (op == CigarOperator.INSERTION)
					nm += length;
			} else if (op == CigarOperator.SKIPPED_REGION) {
				x += length;
			}
		}
		str.append(u);

		if (calcMD)
			record.setAttribute(SAMTag.MD.name(), str.toString());
		if (calcNM)
			record.setAttribute(SAMTag.NM.name(), nm);
	}

}
