package net.sf.cram.cg;


import htsjdk.samtools.SAMUtils;

class EvidenceRecord {
	public String IntervalId = null;
	public String Chromosome = null;
	public String Slide = null;
	public String Lane = null;
	public String FileNumInLane = null;
	public String DnbOffsetInLaneFile = null;
	public String AlleleIndex = null;
	public String Side = null;
	public String Strand = null;
	public String OffsetInAllele = null;
	public String AlleleAlignment = null;
	public String OffsetInReference = null;
	public String ReferenceAlignment = null;
	public String MateOffsetInReference = null;
	public String MateReferenceAlignment = null;
	public String MappingQuality = null;
	public String ScoreAllele0 = null;
	public String ScoreAllele1 = null;
	public String ScoreAllele2 = null;
	public String Sequence = null;
	public String Scores = null;

	public int interval = 0;
	public int side = 0;
	public boolean negativeStrand = false;
	public int mapq = 0;
	public int pos = 0;

	public String line = null;
	public String name = null;

	public String getReadName() {
		return name;
	}

	public static EvidenceRecord fromString(String line) {
		String[] words = line.split("\t");

		EvidenceRecord r = new EvidenceRecord();
		r.line = line;
		int i = 0;
		r.IntervalId = words[i++];
		r.Chromosome = words[i++];
		r.Slide = words[i++];
		r.Lane = words[i++];
		r.FileNumInLane = words[i++];
		r.DnbOffsetInLaneFile = words[i++];
		r.AlleleIndex = words[i++];
		r.Side = words[i++];
		r.Strand = words[i++];
		r.OffsetInAllele = words[i++];
		r.AlleleAlignment = words[i++];
		r.OffsetInReference = words[i++];
		r.ReferenceAlignment = words[i++];
		r.MateOffsetInReference = words[i++];
		r.MateReferenceAlignment = words[i++];
		r.MappingQuality = words[i++];
		r.ScoreAllele0 = words[i++];
		r.ScoreAllele1 = words[i++];
		r.ScoreAllele2 = words[i++];
		r.Sequence = words[i++];
		r.Scores = words[i++];

		r.interval = Integer.valueOf(r.IntervalId);
		r.negativeStrand = "+".equals(r.Strand) ? false : true;
		r.side = "L".equals(r.Side) ? 0 : 1;
		r.name = String.format("%s-%s-%s:%s", r.Slide, r.Lane, r.FileNumInLane, r.DnbOffsetInLaneFile);
		r.mapq = Integer.valueOf(SAMUtils.fastqToPhred(r.MappingQuality.charAt(0)));
		r.pos = Integer.valueOf(r.OffsetInReference);

		return r;
	}
}