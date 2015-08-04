package net.sf.cram.cg;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMUtils;
import htsjdk.samtools.SamPairUtil;
import htsjdk.samtools.util.SequenceUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

class Read {
	EvidenceRecord evidenceRecord;

	ByteBuffer baseBuf = ByteBuffer.allocate(1024);
	ByteBuffer scoreBuf = ByteBuffer.allocate(1024);

	HalfDnb firstHalf = new HalfDnb();
	HalfDnb secondHalf = new HalfDnb();

	boolean negative;

	static class HalfDnb {
		List<CigEl> cgCigElList = new ArrayList<CigEl>();

		ByteBuffer baseBuf;
		ByteBuffer scoreBuf;

		ByteBuffer readBasesBuf = ByteBuffer.allocate(1024);
		ByteBuffer readScoresBuf = ByteBuffer.allocate(1024);
		ByteBuffer gcBuf = ByteBuffer.allocate(1024);
		ByteBuffer gqBuf = ByteBuffer.allocate(1024);
		ByteBuffer gsBuf = ByteBuffer.allocate(1024);

		LinkedList<CigEl> gcList = new LinkedList<CigEl>();
		LinkedList<CigarElement> cigarElements = new LinkedList<CigarElement>();
		LinkedList<CigEl> samCigarElements = new LinkedList<CigEl>();

		void clear() {
			cgCigElList.clear();
			readBasesBuf.clear();
			readScoresBuf.clear();
			gcBuf.clear();
			gsBuf.clear();
			gqBuf.clear();
			gcList.clear();
			readBasesBuf.clear();
			cigarElements.clear();
			samCigarElements.clear();
		}
	}

	void parse() {
		parseAlignments();
		parse(firstHalf);
		parse(secondHalf);
	}

	private static void parse(HalfDnb half) {
		Splitter.split(half);
		half.samCigarElements = Splitter.pack(half.samCigarElements);
		half.readBasesBuf.flip();
		half.readScoresBuf.flip();
		half.gsBuf.flip();
		half.gqBuf.flip();
	}

	void reset(EvidenceRecord evidenceRecord) {
		this.evidenceRecord = evidenceRecord;

		negative = "-".equals(evidenceRecord.Strand);

		baseBuf.clear();
		scoreBuf.clear();
		if (evidenceRecord.Strand.equals("+")) {
			baseBuf.put(evidenceRecord.Sequence.getBytes());
			scoreBuf.put(SAMUtils.fastqToPhred(evidenceRecord.Scores));
		} else {
			byte[] bytes = evidenceRecord.Sequence.getBytes();
			SequenceUtil.reverseComplement(bytes);
			baseBuf.put(bytes);
			bytes = SAMUtils.fastqToPhred(evidenceRecord.Scores);
			SequenceUtil.reverseQualities(bytes);
			scoreBuf.put(bytes);
		}
		baseBuf.flip();
		scoreBuf.flip();

		firstHalf.clear();
		secondHalf.clear();
	}

	private void parseAlignments() {
		Utils.parseCigarInto(evidenceRecord.ReferenceAlignment, firstHalf.cgCigElList);
		int len = 0;
		for (CigEl e : firstHalf.cgCigElList) {
			switch (e.op) {
			case 'B':
				break;

			default:
				len += CigarOperator.characterToEnum(e.op).consumesReadBases() ? e.len : 0;
				break;
			}
		}

		if ((evidenceRecord.side == 1 && !evidenceRecord.negativeStrand)
				|| (evidenceRecord.side == 0 && evidenceRecord.negativeStrand)) {
			secondHalf.baseBuf = Utils.slice(baseBuf, 0, len);
			secondHalf.scoreBuf = Utils.slice(scoreBuf, 0, len);
			firstHalf.baseBuf = Utils.slice(baseBuf, len, baseBuf.limit());
			firstHalf.scoreBuf = Utils.slice(scoreBuf, len, scoreBuf.limit());
		} else {
			firstHalf.baseBuf = Utils.slice(baseBuf, 0, len);
			firstHalf.scoreBuf = Utils.slice(scoreBuf, 0, len);
			secondHalf.baseBuf = Utils.slice(baseBuf, len, baseBuf.limit());
			secondHalf.scoreBuf = Utils.slice(scoreBuf, len, scoreBuf.limit());
		}

		Utils.parseCigarInto(evidenceRecord.MateReferenceAlignment, secondHalf.cgCigElList);
	}

	SAMRecord firstSAMRecord(SAMFileHeader header) {
		SAMRecord r = new SAMRecord(header);
		r.setReadName(evidenceRecord.getReadName());
		r.setReferenceName(evidenceRecord.Chromosome);
		r.setAlignmentStart(Integer.valueOf(evidenceRecord.OffsetInReference) + 1);
		r.setMappingQuality(Integer.valueOf(evidenceRecord.ScoreAllele0));
		r.setReadPairedFlag(true);
		r.setReadUnmappedFlag(false);
		r.setReadNegativeStrandFlag(negative);
		r.setFirstOfPairFlag(evidenceRecord.side == 0);
		r.setSecondOfPairFlag(!r.getFirstOfPairFlag());

		r.setCigar(new Cigar(Utils.toCigarOperatorList(firstHalf.samCigarElements)));

		r.setReadBases(Utils.toByteArray(firstHalf.readBasesBuf));
		r.setBaseQualities(Utils.toByteArray(firstHalf.readScoresBuf));

		r.setAttribute("GC", Utils.toString(firstHalf.gcList));
		r.setAttribute("GS", Utils.toString(firstHalf.gsBuf));
		r.setAttribute("GQ", SAMUtils.phredToFastq(Utils.toByteArray(firstHalf.gqBuf)));

		return r;
	}

	SAMRecord secondSAMRecord(SAMFileHeader header) {
		SAMRecord r = new SAMRecord(header);
		r.setReadName(evidenceRecord.getReadName());
		r.setReferenceName(evidenceRecord.Chromosome);
		r.setAlignmentStart(Integer.valueOf(evidenceRecord.MateOffsetInReference) + 1);
		r.setMappingQuality(Integer.valueOf(evidenceRecord.ScoreAllele0));
		r.setReadPairedFlag(true);
		r.setReadUnmappedFlag(false);
		r.setReadNegativeStrandFlag(negative);
		r.setFirstOfPairFlag(evidenceRecord.side == 1);
		r.setSecondOfPairFlag(!r.getFirstOfPairFlag());

		r.setCigar(new Cigar(Utils.toCigarOperatorList(secondHalf.samCigarElements)));

		r.setReadBases(Utils.toByteArray(secondHalf.readBasesBuf));
		r.setBaseQualities(Utils.toByteArray(secondHalf.readScoresBuf));

		r.setAttribute("GC", Utils.toString(secondHalf.gcList));
		r.setAttribute("GS", Utils.toString(secondHalf.gsBuf));
		r.setAttribute("GQ", SAMUtils.phredToFastq(Utils.toByteArray(secondHalf.gqBuf)));

		return r;
	}

	SAMRecord[] toSAMRecord(SAMFileHeader header) {

		SAMRecord first = firstSAMRecord(header);
		SAMRecord second = secondSAMRecord(header);

		SamPairUtil.setMateInfo(first, second, header);

		first.setProperPairFlag(true);
		second.setProperPairFlag(true);

		return new SAMRecord[] { first, second };
	}

}