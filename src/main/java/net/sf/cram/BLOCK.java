package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import net.sf.block.ExposedByteArrayOutputStream;
import net.sf.cram.encoding.DataReaderFactory;
import net.sf.cram.encoding.DataWriterFactory;
import net.sf.cram.encoding.Reader;
import net.sf.cram.encoding.Writer;
import net.sf.cram.encoding.read_features.BaseChange;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.cram.stats.CompressionHeaderFactory;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import uk.ac.ebi.ena.sra.cram.io.DefaultBitInputStream;
import uk.ac.ebi.ena.sra.cram.io.DefaultBitOutputStream;

public class BLOCK {

	public static class Container {
		public int sequenceId = -1;
		public long alignmentStart = -1;
		public long alignmentSpan = -1;

		public int nofRecords = -1;

		public CompressionHeader h;

		public Slice[] slices;
	}

	public static class Slice {
		public int sequenceId = -1;
		public long alignmentStart = -1;
		public long alignmentSpan = -1;

		public int nofRecords = -1;

		public BlockContentType contentType;
		public Block coreBlock;
		public Map<Integer, Block> external;
	}

	public static class Block {
		public BlockContentType contentType;
		public byte[] content;
	}

	public enum BlockContentType {
		FILE_HEADER, COMPRESSION_HEADER, MAPPED_SLICE, UNMAPPED_SLICE;
	}

	public static List<CramRecord> records(CompressionHeader h, Container c,
			SAMFileHeader fileHeader) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		List<CramRecord> records = new ArrayList<>();
		for (Slice s : c.slices)
			records.addAll(records(s, h, fileHeader));

		restoreTemplatesByDistance(records);
		restoreTemplatesByName(records);
		normalize(records);

		return records;
	}

	private static void restoreTemplatesByDistance(List<CramRecord> records) {
		for (int i = 0; i < records.size(); i++) {
			CramRecord r = records.get(i);
			long d = r.getRecordsToNextFragment();
			if (!r.detached && r.next == null && d > 0
					&& i + d < records.size()) {
				CramRecord mate = records.get((int) (i + d));
				r.next = mate;
				mate.previous = r;
			}
		}
	}

	private static void restoreTemplatesByName(List<CramRecord> records) {
		Map<String, CramRecord> map = new HashMap<String, CramRecord>();
		for (CramRecord r : records) {
			if (r.detached) {
				String n = r.getReadName();
				CramRecord next = map.get(n);
				if (next != null) {
					next = r;
					r.previous = next;
					map.remove(n);
				} else
					map.put(n, r);
			}
		}
	}

	private static void normalize(List<CramRecord> records) {
		for (CramRecord r : records) {
			// update template info based on '.next' and '.prev' pointers
		}
	}

	public static List<CramRecord> records(Slice s, CompressionHeader h,
			SAMFileHeader fileHeader) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		SAMSequenceRecord sequence = fileHeader.getSequence(s.sequenceId);
		String seqName = sequence.getSequenceName();
		DataReaderFactory f = new DataReaderFactory();
		Map<Integer, InputStream> inputMap = new HashMap<>();
		for (Integer exId : s.external.keySet()) {
			inputMap.put(exId, new ByteArrayInputStream(
					s.external.get(exId).content));
		}

		Reader reader = f.buildReader(new DefaultBitInputStream(
				new ByteArrayInputStream(s.coreBlock.content)), inputMap, h);

		List<CramRecord> records = new ArrayList<>();
		for (int i = 0; i < s.nofRecords; i++) {
			CramRecord r = new CramRecord();
			r.setSequenceName(seqName);
			r.sequenceId = sequence.getSequenceIndex();

			reader.read(r);
			records.add(r);

		}

		return records;
	}

	public static List<CramRecord> read(CramRecordCodec codec, byte[] core,
			Map<Integer, Block> external) {
		return null;
	}

	public static List<CramRecord> records(Container c, int sequence,
			long alignmentStart, CompressionHeader h, SAMFileHeader fileHeader) {
		return null;
	}

	// public static class PreservationPolicy {
	// public boolean captureReadNames = false;
	// public boolean captureUnplacedQualityScores = false;
	// public boolean capturePlacedQualityScores = false;
	// public boolean captureMappedQualityScore = false;
	// public boolean captureUnmappedQualityScore = false;
	// public boolean captureDeletionFlankingQualityScore = false;
	// public boolean captureAllTags = false;
	// public List<String> captureTags = new ArrayList<>();
	//
	// public boolean captureInsertionQualityScore = false;
	// public boolean captureInsertions = true;
	// public int capturePiledQualityScoreThreshold = 0;
	// public byte captureQualityScoresForMappingQualityHigherThen =
	// Byte.MAX_VALUE;
	// public byte captureQualityScoresForMappingQualityLowerThen = 0;
	//
	// public List<String> captureSequences = new ArrayList<>();
	// }

	public static enum ReadCategoryType {
		UNPLACED('P'), HIGHER_MAPPING_SCORE('M'), LOWER_MAPPING_SCORE('m');

		public char code;

		ReadCategoryType(char code) {
			this.code = code;
		}
	}

	public static class ReadCategory {
		public final ReadCategoryType type;
		public final int param;

		private ReadCategory(ReadCategoryType type, int param) {
			this.type = type;
			this.param = param;
		}

		public static ReadCategory unplaced() {
			return new ReadCategory(ReadCategoryType.UNPLACED, -1);
		};

		public static ReadCategory higher_then_mapping_score(int score) {
			return new ReadCategory(ReadCategoryType.HIGHER_MAPPING_SCORE,
					score);
		};

		public static ReadCategory lower_then_mapping_score(int score) {
			return new ReadCategory(ReadCategoryType.LOWER_MAPPING_SCORE, score);
		};
	}

	public static enum BaseCategoryType {
		MATCH('R'), MISMATCH('N'), FLANKING_DELETION('D'), PILEUP('P'), LOWER_COVERAGE(
				'X');

		public char code;

		BaseCategoryType(char code) {
			this.code = code;
		}
	}

	public static class BaseCategory {
		public final BaseCategoryType type;
		public final int param;

		private BaseCategory(BaseCategoryType type, int param) {
			this.type = type;
			this.param = param;
		}

		public static BaseCategory match() {
			return new BaseCategory(BaseCategoryType.MATCH, -1);
		}

		public static BaseCategory mismatch() {
			return new BaseCategory(BaseCategoryType.MISMATCH, -1);
		}

		public static BaseCategory flanking_deletion() {
			return new BaseCategory(BaseCategoryType.FLANKING_DELETION, -1);
		}

		public static BaseCategory pileup(int threshold) {
			return new BaseCategory(BaseCategoryType.PILEUP, threshold);
		}

		public static BaseCategory lower_than_coverage(int coverage) {
			return new BaseCategory(BaseCategoryType.LOWER_COVERAGE, coverage);
		};
	}

	public static enum QualityScoreTreatmentType {
		PRESERVE, BIN, DROP;
	}

	public static class QualityScoreTreatment {
		public final QualityScoreTreatmentType type;
		public final int param;

		private QualityScoreTreatment(QualityScoreTreatmentType type, int param) {
			this.type = type;
			this.param = param;
		}

		public static QualityScoreTreatment preserve() {
			return new QualityScoreTreatment(
					QualityScoreTreatmentType.PRESERVE, 40);
		}

		public static QualityScoreTreatment drop() {
			return new QualityScoreTreatment(QualityScoreTreatmentType.DROP, 40);
		}

		public static QualityScoreTreatment bin(int bins) {
			return new QualityScoreTreatment(QualityScoreTreatmentType.BIN,
					bins);
		}
	}

	public static class PreservationPolicy {
		ReadCategory readCategory;
		List<BaseCategory> baseCategories = new ArrayList<>();

		QualityScoreTreatment treatment;
	}

	public static List<PreservationPolicy> policyList = new ArrayList<>();
	static {
		// these should be sorted by qs treatment from none to max!

		// R8X10-R40X5-N40-U40
		PreservationPolicy c1 = new PreservationPolicy();
		c1.baseCategories.add(BaseCategory.lower_than_coverage(10));
		c1.baseCategories.add(BaseCategory.match());
		c1.treatment = QualityScoreTreatment.bin(8);
		policyList.add(c1);

		PreservationPolicy c2 = new PreservationPolicy();
		c1.baseCategories.add(BaseCategory.lower_than_coverage(5));
		c1.baseCategories.add(BaseCategory.match());
		c2.treatment = QualityScoreTreatment.bin(40);
		policyList.add(c2);

		PreservationPolicy c3 = new PreservationPolicy();
		c1.baseCategories.add(BaseCategory.mismatch());
		c3.treatment = QualityScoreTreatment.bin(40);
		policyList.add(c3);

		PreservationPolicy c4 = new PreservationPolicy();
		c4.readCategory = ReadCategory.unplaced();
		c4.treatment = QualityScoreTreatment.bin(40);
		policyList.add(c4);
	}
	static {
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

	public static final void applyBinning(byte[] scores) {
		for (int i = 0; i < scores.length; i++)
			scores[i] = Illumina_binning_matrix[scores[i]];
	}

	public static final byte applyTreatment(byte score, QualityScoreTreatment t) {
		switch (t.type) {
		case BIN:
			return Illumina_binning_matrix[score - 33];
		case DROP:
			return -1;
		case PRESERVE:
			return score;

		}
		throw new RuntimeException("Unknown quality score treatment type: "
				+ t.type.name());
	}

	public static void addQS(SAMRecord s, CramRecord r, ReferenceTracks t,
			List<PreservationPolicy> pp) {
		byte[] scores = new byte[s.getReadLength()];
		Arrays.fill(scores, (byte) -1);
		for (PreservationPolicy p : pp)
			addQS(s, r, scores, t, p);

		if (!r.forcePreserveQualityScores) {
			for (int i = 0; i < scores.length; i++) {
				if (scores[i] > -1)
					r.getReadFeatures().add(new BaseQualityScore(i, scores[i]));
			}
			Collections
					.sort(r.getReadFeatures(), readFeaturePositionComparator);
		}
	}

	private static Comparator<ReadFeature> readFeaturePositionComparator = new Comparator<ReadFeature>() {

		@Override
		public int compare(ReadFeature o1, ReadFeature o2) {
			return o1.getPosition() - o2.getPosition();
		}
	};

	public static void addQS(SAMRecord s, CramRecord r, byte[] scores,
			ReferenceTracks t, PreservationPolicy p) {
		int alSpan = s.getAlignmentEnd() - s.getAlignmentStart();
		t.ensureRange(s.getAlignmentStart(), alSpan);
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

		for (BaseCategory c : p.baseCategories) {
			int pos;
			switch (c.type) {
			case FLANKING_DELETION:
				pos = 0;
				for (CigarElement ce : s.getCigar().getCigarElements()) {
					if (ce.getOperator() == CigarOperator.D) {
						if (pos > 0)
							mask[pos - 1] = true;
						if (pos < mask.length)
							mask[pos + 1] = true;
					}

					pos += ce.getOperator().consumesReadBases() ? ce
							.getLength() : 0;
				}
				break;
			case MATCH:
			case MISMATCH:
				pos = 0;
				for (CigarElement ce : s.getCigar().getCigarElements()) {
					switch (ce.getOperator()) {
					case M:
					case X:
					case EQ:
						for (int i = 0; i < ce.getLength(); i++) {
							boolean match = s.getReadBases()[pos + i] == t
									.baseAt(s.getAlignmentStart() + pos + i);
							if ((c.type == BaseCategoryType.MATCH && match)
									|| (c.type == BaseCategoryType.MISMATCH && !match)) {
								mask[pos] = true;
							}
						}
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

	public static class ReferenceTracks {
		private int sequenceId;
		private String sequenceName;
		private byte[] reference;

		private int position;

		// a copy of ref bases for the given range:
		private final byte[] bases;
		private final short[] coverage;
		private final short[] mismatches;

		public ReferenceTracks(int sequenceId, String sequenceName,
				byte[] reference, int windowSize) {
			this.sequenceId = sequenceId;
			this.sequenceName = sequenceName;
			this.reference = reference;
			bases = new byte[windowSize];
			coverage = new short[windowSize];
			mismatches = new short[windowSize];

			reset();
		}

		public int getSequenceId() {
			return sequenceId;
		}

		public String getSequenceName() {
			return sequenceName;
		}

		public int getWindowPosition() {
			return position;
		}

		public int getWindowLength() {
			return bases.length;
		}

		public int getReferenceLength() {
			return reference.length;
		}

		public void moveForwardTo(int newPos) {
			if (newPos < position)
				throw new RuntimeException(
						"Cannot shift to smaller position on the reference.");
			if (newPos == position)
				return;

			System.arraycopy(reference, newPos, bases, 0, bases.length);

			if (newPos > position && position + bases.length - newPos > 0) {
				System.arraycopy(coverage, (newPos - position), coverage, 0,
						(position - newPos + coverage.length));
				System.arraycopy(mismatches, (newPos - position), mismatches,
						0, (position - newPos + coverage.length));
			} else {
				Arrays.fill(coverage, (short) 0);
				Arrays.fill(mismatches, (short) 0);
			}

			this.position = newPos;
		}

		public void reset() {
			System.arraycopy(reference, position, bases, 0, bases.length);
			Arrays.fill(coverage, (short) 0);
			Arrays.fill(mismatches, (short) 0);
		}

		public void ensureRange(int start, int length) {
			if (length > bases.length)
				throw new RuntimeException("Requested window is too big: "
						+ length);
			if (start < position)
				throw new RuntimeException("Cannot move the window backwords: "
						+ start);

			if (start + length > position + bases.length)
				moveForwardTo(start);
		}

		public final byte baseAt(int pos) {
			return bases[pos - this.position];
		}

		public final short coverageAt(int pos) {
			return coverage[pos - this.position];
		}

		public final short mismatchesAt(int pos) {
			return mismatches[pos - this.position];
		}

		public final void addCoverage(int pos, int amount) {
			coverage[pos - this.position] += amount;
		}

		public final void addMismatches(int pos, int amount) {
			mismatches[pos - this.position] += amount;
		}
	}

	public static void applyPolicies(List<ReadFeature> rfs) {
		ReferenceTracks tracks = new ReferenceTracks(1, "seq1",
				"AAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes(), 10);

		for (PreservationPolicy p : policyList) {
			// applye theme here somehow...
		}
	}

	public static Container writeContainer(List<CramRecord> records,
			SAMFileHeader fileHeader) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		// get stats, create compression header and slices
		long time1 = System.nanoTime();
		CompressionHeader h = new CompressionHeaderFactory().build(records);
		long time2 = System.nanoTime();
		System.out.println("Compression header built in " + (time2 - time1)
				/ 1000000 + " ms.");
		h.mappedQualityScoreIncluded = true;
		h.unmappedQualityScoreIncluded = true;
		h.readNamesIncluded = true;

		int recordsPerSlice = 10000;

		List<Slice> slices = new ArrayList<>();

		Container c = new Container();
		c.h = h;
		c.nofRecords = records.size();
		for (int i = 0; i < records.size(); i += recordsPerSlice) {
			List<CramRecord> sliceRecords = records.subList(i,
					Math.min(records.size(), i + recordsPerSlice));
			Slice slice = writeSlice(sliceRecords, h, fileHeader);
			slices.add(slice);

			// assuming one sequence per container max:
			if (c.sequenceId == -1 && slice.sequenceId != -1) {
				c.sequenceId = slice.sequenceId;
				c.alignmentStart = slice.alignmentStart;
				c.alignmentSpan = slice.alignmentSpan - c.alignmentStart;
			}
		}

		c.slices = (Slice[]) slices.toArray(new Slice[slices.size()]);
		return c;
	}

	public static Slice writeSlice(List<CramRecord> records,
			CompressionHeader h, SAMFileHeader fileHeader)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		Map<Integer, ExposedByteArrayOutputStream> map = new HashMap<>();
		for (int id : h.externalIds) {
			map.put(id, new ExposedByteArrayOutputStream());
		}

		DataWriterFactory f = new DataWriterFactory();
		ExposedByteArrayOutputStream bitBAOS = new ExposedByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(bitBAOS);
		Writer writer = f.buildWriter(bos, map, h);

		Slice slice = new Slice();
		slice.nofRecords = records.size();
		for (CramRecord r : records) {
			writer.write(r);

			// if (!r.isReadMapped())
			// continue;

			if (slice.alignmentStart == -1) {
				slice.alignmentStart = r.getAlignmentStart();
				slice.sequenceId = r.sequenceId;
			}

			slice.alignmentSpan = r.getAlignmentStart() - slice.alignmentStart;
		}

		slice.contentType = slice.alignmentSpan > -1 ? BlockContentType.MAPPED_SLICE
				: BlockContentType.UNMAPPED_SLICE;

		slice.coreBlock = new Block();
		slice.coreBlock.content = bitBAOS.getBuffer();
		bos.close();

		slice.external = new HashMap<>();
		for (Integer i : map.keySet()) {
			ExposedByteArrayOutputStream os = map.get(i);

			Block externalBlock = new Block();
			externalBlock.content = os.getBuffer();
			slice.external.put(i, externalBlock);
		}

		return slice;
	}

	private static void randomStressTest() throws IOException,
			IllegalArgumentException, IllegalAccessException {
		SAMFileHeader samFileHeader = new SAMFileHeader();
		SAMSequenceRecord sequenceRecord = new SAMSequenceRecord("chr1", 100);
		samFileHeader.addSequence(sequenceRecord);

		long baseCount = 0;
		Random random = new Random();
		List<CramRecord> records = new ArrayList<>();
		for (int i = 0; i < 100000; i++) {
			int len = random.nextInt(100) + 50;
			byte[] bases = new byte[len];
			byte[] scores = new byte[len];
			for (int p = 0; p < len; p++) {
				bases[p] = "ACGT".getBytes()[random.nextInt(4)];
				scores[p] = (byte) (33 + random.nextInt(40));
			}

			CramRecord record = new CramRecord();
			record.setReadBases(bases);
			record.setQualityScores(scores);
			record.setReadLength(record.getReadBases().length);
			record.setFlags(random.nextInt(1000));
			record.alignmentStartOffsetFromPreviousRecord = random.nextInt(200);

			byte[] name = new byte[random.nextInt(5) + 5];
			for (int p = 0; p < name.length; p++)
				name[p] = (byte) (65 + random.nextInt(10));
			record.setReadName(new String(name));

			record.setSequenceName(sequenceRecord.getSequenceName());
			record.sequenceId = sequenceRecord.getSequenceIndex();
			record.setReadMapped(random.nextBoolean());
			record.resetFlags();
			record.setReadFeatures(new ArrayList<ReadFeature>());
			record.setMappingQuality((byte) random.nextInt(40));

			if (record.isReadMapped()) {
				byte[] ops = new byte[] { SubstitutionVariation.operator,
						DeletionVariation.operator,
						InsertionVariation.operator, ReadBase.operator,
						BaseQualityScore.operator, InsertBase.operator };
				int prevPos = 0;
				do {
					int newPos = prevPos + random.nextInt(30);
					if (newPos >= record.getReadLength())
						break;
					prevPos = newPos;

					byte op = ops[random.nextInt(ops.length)];
					switch (op) {
					case SubstitutionVariation.operator:
						SubstitutionVariation sv = new SubstitutionVariation();
						sv.setPosition(newPos);
						sv.setBaseChange(new BaseChange(random.nextInt(4)));
						record.getReadFeatures().add(sv);
						break;
					case DeletionVariation.operator:
						DeletionVariation dv = new DeletionVariation();
						dv.setPosition(newPos);
						dv.setLength(random.nextInt(10));
						record.getReadFeatures().add(dv);
						break;
					case InsertionVariation.operator:
						InsertionVariation iv = new InsertionVariation();
						iv.setPosition(newPos);
						byte[] seq = new byte[random.nextInt(10) + 1];
						for (int p = 0; p < seq.length; p++)
							seq[p] = "ACGT".getBytes()[random.nextInt(4)];
						iv.setSequence(seq);
						record.getReadFeatures().add(iv);
						break;
					case ReadBase.operator:
						ReadBase rb = new ReadBase(newPos,
								"ACGT".getBytes()[random.nextInt(4)],
								(byte) (33 + random.nextInt(40)));
						record.getReadFeatures().add(rb);
						break;
					case BaseQualityScore.operator:
						BaseQualityScore qs = new BaseQualityScore(newPos,
								(byte) (33 + random.nextInt(40)));
						record.getReadFeatures().add(qs);
						break;
					case InsertBase.operator:
						InsertBase ib = new InsertBase();
						ib.setPosition(newPos);
						ib.setBase("ACGT".getBytes()[random.nextInt(4)]);
						record.getReadFeatures().add(ib);
						break;

					default:
						break;
					}
				} while (prevPos < record.getReadLength());
			}

			if (!record.isLastFragment())
				record.setRecordsToNextFragment(random.nextInt(10000));
			records.add(record);
			baseCount += record.getReadLength();
		}

		long time1 = System.nanoTime();
		Container c = writeContainer(records, samFileHeader);
		long time2 = System.nanoTime();
		System.out.println("Container written in " + (time2 - time1) / 1000000
				+ " milli seconds");

		time1 = System.nanoTime();
		List<CramRecord> readRecords = records(c.h, c, samFileHeader);
		time2 = System.nanoTime();
		System.out.println("Container read in " + (time2 - time1) / 1000000
				+ " milli seconds");

		for (CramRecord rr : readRecords) {
			System.out.println(rr);
			break;
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(baos);
		long size = 0;
		for (Slice s : c.slices) {
			size += s.coreBlock.content.length;
			gos.write(s.coreBlock.content);
			for (Block b : s.external.values()) {
				size += b.content.length;
				gos.write(b.content);
			}
		}
		gos.close();
		System.out.println("Bases: " + baseCount);
		System.out.printf("Uncompressed container size: %d; %.2f\n", size, size
				* 8f / baseCount);
		System.out.printf("Compressed container size: %d; %.2f\n", baos.size(),
				baos.size() * 8f / baseCount);
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IllegalAccessException, IOException {

		SAMFileReader samFileReader = new SAMFileReader(
				new File(
						"c:/temp/HG00096.mapped.illumina.mosaik.GBR.exome.20110411.chr20.bam"));
		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(new File(
						"c:/temp/human_g1k_v37.fasta"));

		ReferenceSequence sequence = null;
		{
			String seqName = null;
			SAMRecordIterator iterator = samFileReader.iterator();
			SAMRecord samRecord = iterator.next();
			seqName = samRecord.getReferenceName();
			iterator.close();
			sequence = referenceSequenceFile.getSequence(seqName);
		}

		int maxRecords = 50000;
		List<SAMRecord> samRecords = new ArrayList<>(maxRecords);

		int alStart = Integer.MAX_VALUE;
		int alEnd = 0;
		long baseCount = 0 ;
		SAMRecordIterator iterator = samFileReader.iterator();
		do {
			SAMRecord samRecord = iterator.next();
			if (!samRecord.getReferenceName().equals(sequence.getName())
					|| samRecords.size() >= maxRecords)
				break;

			baseCount += samRecord.getReadLength() ;
			samRecords.add(samRecord);
			if (samRecord.getAlignmentStart() > 0
					&& alStart > samRecord.getAlignmentStart())
				alStart = samRecord.getAlignmentStart();
			if (alEnd < samRecord.getAlignmentEnd())
				alEnd = samRecord.getAlignmentEnd();
		} while (iterator.hasNext());

		ReferenceTracks tracks = new ReferenceTracks(sequence.getContigIndex(),
				sequence.getName(), sequence.getBases(), alEnd - alStart + 1);
		tracks.moveForwardTo(alStart);

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(sequence.getBases());
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		List<CramRecord> cramRecords = new ArrayList<>(maxRecords);
		int prevAlStart = 1;
		int index = 0;
		for (SAMRecord samRecord : samRecords) {
			CramRecord cramRecord = f.createCramRecord(samRecord);
			cramRecord.index = index++;
			cramRecord.alignmentStartOffsetFromPreviousRecord = samRecord
					.getAlignmentStart() - prevAlStart;
			prevAlStart = samRecord.getAlignmentStart();

			cramRecords.add(cramRecord);
			int refPos = samRecord.getAlignmentStart();
			int readPos = 0;
			for (CigarElement ce : samRecord.getCigar().getCigarElements()) {
				if (ce.getOperator().consumesReferenceBases()) {
					for (int i = 0; i < ce.getLength(); i++)
						tracks.addCoverage(refPos + i, 1);
				}
				switch (ce.getOperator()) {
				case M:
				case X:
				case EQ:
					for (int i = readPos; i < ce.getLength(); i++) {
						byte readBase = samRecord.getReadBases()[readPos + i];
						byte refBase = tracks.baseAt(refPos + i);
						if (readBase != refBase)
							tracks.addMismatches(refPos + i, 1);
					}
					break;

				default:
					break;
				}

				readPos += ce.getOperator().consumesReadBases() ? ce.getLength() : 0;
				refPos += ce.getOperator().consumesReferenceBases() ? ce.getLength() : 0;
			}

			addQS(samRecord, cramRecord, tracks, policyList);
		}

		// mating:
		Map<String, CramRecord> mateMap = new TreeMap<String, CramRecord>();
		for (CramRecord r : cramRecords) {
			if (r.lastFragment) {
				r.recordsToNextFragment = -1;
				continue;
			}

			String name = r.getReadName();
			CramRecord mate = mateMap.get(name);
			if (mate == null) {
				mateMap.put(name, r);
				continue;
			}

			mate.recordsToNextFragment = r.index - mate.index;
		}
		for (CramRecord r : cramRecords) {
			if (!r.lastFragment && r.next == null)
				r.detached = true;
		}

		for (int i=0; i<Math.min(cramRecords.size(), 10); i++)
			System.out.println(cramRecords.get(i).toString());
		
		System.out.println();
		
		long time1 = System.nanoTime();
		Container c = writeContainer(cramRecords, samFileReader.getFileHeader());
		long time2 = System.nanoTime();
		System.out.println("Container written in " + (time2 - time1) / 1000000
				+ " milli seconds");

		time1 = System.nanoTime();
		List<CramRecord> newRecords = records(c.h, c,
				samFileReader.getFileHeader());
		time2 = System.nanoTime();
		System.out.println("Container read in " + (time2 - time1) / 1000000
				+ " milli seconds");

		
		for (int i=0; i<Math.min(newRecords.size(), 10); i++)
			System.out.println(newRecords.get(i).toString());
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(baos);
		long size = 0;
		for (Slice s : c.slices) {
			size += s.coreBlock.content.length;
			gos.write(s.coreBlock.content);
			for (Block b : s.external.values()) {
				size += b.content.length;
				gos.write(b.content);
			}
		}
		gos.close();
		System.out.println("Bases: " + baseCount);
		System.out.printf("Uncompressed container size: %d; %.2f\n", size, size
				* 8f / baseCount);
		System.out.printf("Compressed container size: %d; %.2f\n", baos.size(),
				baos.size() * 8f / baseCount);
	}

	// @formatter:off
	// NCBI binning scheme:
	// Low High Value
	// 0 0 0
	// 1 1 1
	// 2 2 2
	// 3 14 9
	// 15 19 17
	// 20 24 22
	// 25 29 28
	// 30 nolimit 35
	// @formatter:on
	private static byte[] NCBI_binning_matrix = new byte[] {
			// @formatter:off
			0, 1, 2, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 17, 17, 17, 17, 17,
			22, 22, 22, 22,
			22,
			28,
			28,
			28,
			28,
			28,
			// @formatter:on
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
			35 };

	// @formatter:off
	// Illumina binning scheme:
	// 2-9 6
	// 10-19 15
	// 20-24 22
	// 25-29 27
	// 30-34 33
	// 35-39 37
	// ≥40 40
	// @formatter:on
	private static byte[] Illumina_binning_matrix = new byte[] {// @formatter:off
	0, 1, 6, 6, 6, 6, 6, 6, 6, 6, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 22,
			22, 22, 22, 22, 27, 27, 27, 27, 27, 33, 33, 33, 33, 33, 37, 37, 37,
			37, 37, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
			40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
			40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
			40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
			40, 40, 40, 40 };
	// @formatter:on
}
