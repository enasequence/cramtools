package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import net.sf.samtools.SAMFileHeader;
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
		PRESERVE, DROP, BIN;
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
	{
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
		}

		public void reset() {
			System.arraycopy(reference, position, bases, 0, bases.length);
			Arrays.fill(coverage, (short) 0);
			Arrays.fill(mismatches, (short) 0);
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
		long time1 = System.nanoTime() ;
		CompressionHeader h = new CompressionHeaderFactory().build(records);
		long time2 = System.nanoTime() ;
		System.out.println("Compression header built in " + (time2-time1)/1000000 + " ms.");
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
					Math.min(records.size(), i+recordsPerSlice));
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

	public static void main(String[] args) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		SAMFileHeader samFileHeader = new SAMFileHeader();
		SAMSequenceRecord sequenceRecord = new SAMSequenceRecord("chr1", 100);
		samFileHeader.addSequence(sequenceRecord);

		Random random = new Random();
		List<CramRecord> records = new ArrayList<>();
		for (int i = 0; i < 50000; i++) {
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
			record.alignmentStartOffsetFromPreviousRecord = random
					.nextInt(200);

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
						BaseQualityScore qs = new BaseQualityScore(newPos, (byte) (33 + random.nextInt(40)));
						record.getReadFeatures().add(qs);
						break;
					case InsertBase.operator:
						InsertBase ib = new InsertBase();
						ib.setPosition(newPos);
						ib.setBase("ACGT".getBytes()[random.nextInt(4)]) ;
						record.getReadFeatures().add(ib);
						break;

					default:
						break;
					}
				} while (prevPos < record.getReadLength());
			}

			records.add(record);
		}

		long time1 = System.nanoTime() ;
		Container c = writeContainer(records, samFileHeader);
		long time2 = System.nanoTime() ;
		System.out.println("Container written in " + (time2-time1)/1000000 + " milli seconds");
		
		time1 = System.nanoTime() ;
		List<CramRecord> readRecords = records(c.h, c, samFileHeader);
		time2 = System.nanoTime() ;
		System.out.println("Container read in " + (time2-time1)/1000000 + " milli seconds");
		
		for (CramRecord rr : readRecords) {
			System.out.println(rr);
			break ;
		}
		
	}
}
