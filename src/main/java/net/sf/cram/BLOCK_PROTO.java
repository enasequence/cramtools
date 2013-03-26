package net.sf.cram;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.encoding.DataReaderFactory;
import net.sf.cram.encoding.DataReaderFactory.DataReaderWithStats;
import net.sf.cram.encoding.DataWriterFactory;
import net.sf.cram.encoding.Reader;
import net.sf.cram.encoding.Writer;
import net.sf.cram.encoding.read_features.BaseChange;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.Deletion;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.Insertion;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.Substitution;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.stats.CompressionHeaderFactory;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockCompressionMethod;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SubstitutionMatrix;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.CigarElement;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;

public class BLOCK_PROTO {
	private static Log log = Log.getInstance(BLOCK_PROTO.class);
	public static int recordsPerSlice = 10000;

	public static List<CramRecord> getRecords(CompressionHeader h, Container c,
			SAMFileHeader fileHeader, ArrayList<CramRecord> records)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		long time1 = System.nanoTime();
		if (records == null)
			records = new ArrayList<CramRecord>(c.nofRecords);
		Map<String, Long> nanoMap = new TreeMap<String, Long>();
		for (Slice s : c.slices)
			records.addAll(getRecords(s, h, fileHeader, nanoMap));

		long time2 = System.nanoTime();

		c.parseTime = time2 - time1;

		if (log.isEnabled(LogLevel.DEBUG)) {
			for (String key : nanoMap.keySet()) {
				log.debug(String.format("%s: %dms.", key, nanoMap.get(key)
						.longValue() / 1000000));
			}
		}

		return records;
	}

	private static class SwapInputStream extends InputStream {
		private InputStream delegate;

		@Override
		public int read() throws IOException {
			return delegate.read();
		}

		@Override
		public int read(byte[] b) throws IOException {
			return delegate.read(b);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return delegate.read(b, off, len);
		}

		public InputStream getDelegate() {
			return delegate;
		}

		public void setDelegate(InputStream delegate) {
			this.delegate = delegate;
		}
	}

	public static List<CramRecord> getRecords(Slice s, CompressionHeader h,
			SAMFileHeader fileHeader, Map<String, Long> nanoMap)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		String seqName = SAMRecord.NO_ALIGNMENT_REFERENCE_NAME;
		if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			SAMSequenceRecord sequence = fileHeader.getSequence(s.sequenceId);
			seqName = sequence.getSequenceName();
		}
		DataReaderFactory f = new DataReaderFactory();
		Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
		for (Integer exId : s.external.keySet()) {
			inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId)
					.getRawContent()));
		}

		long time = 0;
		Reader reader = f.buildReader(new DefaultBitInputStream(
				new ByteArrayInputStream(s.coreBlock.getRawContent())),
				inputMap, h, s.sequenceId);

		List<CramRecord> records = new ArrayList<CramRecord>();

		long readNanos = 0;
		int prevStart = s.alignmentStart;
		for (int i = 0; i < s.nofRecords; i++) {
			CramRecord r = new CramRecord();
			r.index = i;

			try {
				time = System.nanoTime();
				reader.read(r);
				readNanos += System.nanoTime() - time;
			} catch (EOFException e) {
				e.printStackTrace();
				throw e;
			}

			if (r.sequenceId == s.sequenceId) {
				r.setSequenceName(seqName);
				r.sequenceId = s.sequenceId;
			} else {
				if (r.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
					r.setSequenceName(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME);
				else {
					String name = fileHeader.getSequence(r.sequenceId)
							.getSequenceName();
					r.setSequenceName(name);
				}
			}

			records.add(r);

			if (h.AP_seriesDelta) {
				prevStart += r.alignmentStartOffsetFromPreviousRecord;
				r.setAlignmentStart(prevStart);
			}
		}
		log.debug("Slice records read time: " + readNanos / 1000000);

		Map<String, DataReaderWithStats> statMap = f.getStats(reader);
		for (String key : statMap.keySet()) {
			long value = 0;
			if (!nanoMap.containsKey(key)) {
				nanoMap.put(key, 0L);
				value = 0;
			} else
				value = nanoMap.get(key);
			nanoMap.put(key, value + statMap.get(key).nanos);
		}

		return records;
	}

	public static Container buildContainer(List<CramRecord> records,
			SAMFileHeader fileHeader, boolean preserveReadNames,
			long globalRecordCounter, SubstitutionMatrix substitutionMatrix,
			boolean AP_delta) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		// get stats, create compression header and slices
		long time1 = System.nanoTime();
		CompressionHeader h = new CompressionHeaderFactory().build(records,
				substitutionMatrix);
		h.AP_seriesDelta = AP_delta;
		long time2 = System.nanoTime();

		h.readNamesIncluded = preserveReadNames;
		h.AP_seriesDelta = true;

		List<Slice> slices = new ArrayList<Slice>();

		Container c = new Container();
		c.h = h;
		c.nofRecords = records.size();
		c.globalRecordCounter = globalRecordCounter;
		c.bases = 0;
		c.blockCount = 0;

		long time3 = System.nanoTime();
		long lastGlobalRecordCounter = c.globalRecordCounter;
		for (int i = 0; i < records.size(); i += recordsPerSlice) {
			List<CramRecord> sliceRecords = records.subList(i,
					Math.min(records.size(), i + recordsPerSlice));
			Slice slice = buildSlice(sliceRecords, h, fileHeader);
			slice.globalRecordCounter = lastGlobalRecordCounter;
			lastGlobalRecordCounter += slice.nofRecords;
			c.bases += slice.bases;
			slices.add(slice);

			// assuming one sequence per container max:
			if (c.sequenceId == -1 && slice.sequenceId != -1)
				c.sequenceId = slice.sequenceId;
		}

		long time4 = System.nanoTime();

		c.slices = (Slice[]) slices.toArray(new Slice[slices.size()]);
		calculateAlignmentBoundaries(c);

		c.buildHeaderTime = time2 - time1;
		c.buildSlicesTime = time4 - time3;
		return c;
	}

	public static void calculateAlignmentBoundaries(Container c) {
		int start = Integer.MAX_VALUE;
		int end = Integer.MIN_VALUE;
		for (Slice s : c.slices) {
			if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				start = Math.min(start, s.alignmentStart);
				end = Math.max(end, s.alignmentStart + s.alignmentSpan);
			}
		}

		if (start < Integer.MAX_VALUE) {
			c.alignmentStart = start;
			c.alignmentSpan = end - start;
		}
	}

	private static Slice buildSlice(List<CramRecord> records,
			CompressionHeader h, SAMFileHeader fileHeader)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		Map<Integer, ExposedByteArrayOutputStream> map = new HashMap<Integer, ExposedByteArrayOutputStream>();
		for (int id : h.externalIds) {
			map.put(id, new ExposedByteArrayOutputStream());
		}

		DataWriterFactory f = new DataWriterFactory();
		ExposedByteArrayOutputStream bitBAOS = new ExposedByteArrayOutputStream();
		DefaultBitOutputStream bos = new DefaultBitOutputStream(bitBAOS);

		Slice slice = new Slice();
		slice.nofRecords = records.size();

		int[] seqIds = new int[fileHeader.getSequenceDictionary().size()];
		int minAlStart = Integer.MAX_VALUE;
		int maxAlEnd = SAMRecord.NO_ALIGNMENT_START;
		for (CramRecord r : records) {
			if (r.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
				seqIds[r.sequenceId]++;

			int alStart = r.getAlignmentStart();
			if (alStart != SAMRecord.NO_ALIGNMENT_START) {
				minAlStart = Math.min(alStart, minAlStart);
				maxAlEnd = Math.max(r.calcualteAlignmentEnd(), maxAlEnd);
			}
			slice.bases += r.getReadLength();
		}

		int seqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
		boolean singleSeqId = true;
		for (int i = 0; i < seqIds.length && singleSeqId; i++) {
			if (seqIds[i] > 0) {
				seqId = i++;
				for (; i < seqIds.length && singleSeqId; i++) {
					if (seqIds[i] > 0)
						singleSeqId = false;
				}
			}
		}

		if (!singleSeqId)
			throw new RuntimeException("Multiref slices are not supported.");

		slice.sequenceId = seqId;
		if (minAlStart == Integer.MAX_VALUE) {
			slice.alignmentStart = SAMRecord.NO_ALIGNMENT_START;
			slice.alignmentSpan = 0;
		} else {
			slice.alignmentStart = minAlStart;
			slice.alignmentSpan = maxAlEnd - minAlStart;
		}

		Writer writer = f.buildWriter(bos, map, h, slice.sequenceId);
		int prevAlStart = slice.alignmentStart;
		for (CramRecord r : records) {
			r.alignmentStartOffsetFromPreviousRecord = r.getAlignmentStart()
					- prevAlStart;
			prevAlStart = r.getAlignmentStart();
			writer.write(r);
		}

		slice.contentType = slice.alignmentSpan > -1 ? BlockContentType.MAPPED_SLICE
				: BlockContentType.RESERVED;

		bos.close();
		slice.coreBlock = new Block();
		slice.coreBlock.method = BlockCompressionMethod.RAW.ordinal();
		slice.coreBlock.setRawContent(bitBAOS.toByteArray());
		slice.coreBlock.contentType = BlockContentType.CORE;

		slice.external = new HashMap<Integer, Block>();
		for (Integer i : map.keySet()) {
			ExposedByteArrayOutputStream os = map.get(i);

			Block externalBlock = new Block();
			externalBlock.contentType = BlockContentType.EXTERNAL;
			externalBlock.method = BlockCompressionMethod.GZIP.ordinal();
			externalBlock.contentId = i;

			externalBlock.setRawContent(os.toByteArray());
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
		List<CramRecord> records = new ArrayList<CramRecord>();
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
				byte[] ops = new byte[] { Substitution.operator,
						Deletion.operator, Insertion.operator,
						ReadBase.operator, BaseQualityScore.operator,
						InsertBase.operator };
				int prevPos = 0;
				do {
					int newPos = prevPos + random.nextInt(30);
					if (newPos >= record.getReadLength())
						break;
					prevPos = newPos;

					byte op = ops[random.nextInt(ops.length)];
					switch (op) {
					case Substitution.operator:
						Substitution sv = new Substitution();
						sv.setPosition(newPos);
						sv.setBaseChange(new BaseChange(random.nextInt(4)));
						record.getReadFeatures().add(sv);
						break;
					case Deletion.operator:
						Deletion dv = new Deletion();
						dv.setPosition(newPos);
						dv.setLength(random.nextInt(10));
						record.getReadFeatures().add(dv);
						break;
					case Insertion.operator:
						Insertion iv = new Insertion();
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
		Container c = buildContainer(records, samFileHeader, true, 0, null,
				true);
		long time2 = System.nanoTime();
		System.out.println("Container written in " + (time2 - time1) / 1000000
				+ " milli seconds");

		ArrayList<CramRecord> readRecords = new ArrayList<CramRecord>();
		time1 = System.nanoTime();
		getRecords(c.h, c, samFileHeader, readRecords);
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
			size += s.coreBlock.getCompressedContent().length;
			for (Block b : s.external.values()) {
				size += b.getCompressedContent().length;
			}
		}
		gos.close();
		System.out.println("Bases: " + baseCount);
		System.out.printf("Uncompressed container size: %d; %.2f\n", size, size
				* 8f / baseCount);
		System.out.printf("Compressed container size: %d; %.2f\n", baos.size(),
				baos.size() * 8f / baseCount);
	}

	public static void main(String[] args, boolean preserveReadNames)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		File bamFile = new File(
				"c:/temp/HG00096.mapped.illumina.mosaik.GBR.exome.20110411.chr20.bam");
		SAMFileReader samFileReader = new SAMFileReader(bamFile);
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

		int maxRecords = 100000;
		List<SAMRecord> samRecords = new ArrayList<SAMRecord>(maxRecords);

		int alStart = Integer.MAX_VALUE;
		int alEnd = 0;
		long baseCount = 0;
		SAMRecordIterator iterator = samFileReader.iterator();

		do {
			SAMRecord samRecord = iterator.next();
			if (!samRecord.getReferenceName().equals(sequence.getName()))
				break;

			baseCount += samRecord.getReadLength();
			samRecords.add(samRecord);
			if (samRecord.getAlignmentStart() > 0
					&& alStart > samRecord.getAlignmentStart())
				alStart = samRecord.getAlignmentStart();
			if (alEnd < samRecord.getAlignmentEnd())
				alEnd = samRecord.getAlignmentEnd();

			if (samRecords.size() >= maxRecords)
				break;
		} while (iterator.hasNext());

		ReferenceTracks tracks = new ReferenceTracks(sequence.getContigIndex(),
				sequence.getName(), sequence.getBases(), alEnd - alStart + 100);
		tracks.moveForwardTo(alStart);

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(
				sequence.getBases(), samFileReader.getFileHeader());
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		List<CramRecord> cramRecords = new ArrayList<CramRecord>(maxRecords);
		int prevAlStart = samRecords.get(0).getAlignmentStart();
		int index = 0;
		QualityScorePreservation preservation = new QualityScorePreservation(
				"R8X10-R40X5-N40-U40");
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

				readPos += ce.getOperator().consumesReadBases() ? ce
						.getLength() : 0;
				refPos += ce.getOperator().consumesReferenceBases() ? ce
						.getLength() : 0;
			}

			preservation.addQualityScores(samRecord, cramRecord, tracks);
		}

		// mating:
		Map<String, CramRecord> mateMap = new TreeMap<String, CramRecord>();
		for (CramRecord r : cramRecords) {
			if (r.lastSegment) {
				r.recordsToNextFragment = -1;
				if (r.firstSegment)
					continue;
			}

			String name = r.getReadName();
			CramRecord mate = mateMap.get(name);
			if (mate == null) {
				mateMap.put(name, r);
				continue;
			}

			mate.recordsToNextFragment = r.index - mate.index - 1;
			mate.next = r;
			r.previous = mate;
		}
		for (CramRecord r : cramRecords) {
			if (!r.lastSegment && r.next == null)
				r.detached = true;
		}

		for (int i = 0; i < Math.min(cramRecords.size(), 10); i++)
			System.out.println(cramRecords.get(i).toString());

		System.out.println();

		long time1 = System.nanoTime();
		Container c = buildContainer(cramRecords,
				samFileReader.getFileHeader(), preserveReadNames, 0, null, true);
		long time2 = System.nanoTime();
		System.out.println("Container written in " + (time2 - time1) / 1000000
				+ " milli seconds");

		ArrayList<CramRecord> newRecords = new ArrayList<CramRecord>();
		time1 = System.nanoTime();
		getRecords(c.h, c, samFileReader.getFileHeader(), newRecords);

		mateMap.clear();
		{
			int i = 0;
			for (CramRecord r : newRecords)
				r.index = i++;
		}
		for (CramRecord r : newRecords) {
			if (r.lastSegment) {
				r.recordsToNextFragment = -1;
				if (r.firstSegment)
					continue;
			}

			String name = r.getReadName();
			CramRecord mate = mateMap.get(name);
			if (mate == null) {
				mateMap.put(name, r);
				continue;
			}

			mate.recordsToNextFragment = r.index - mate.index - 1;
			mate.next = r;
			r.previous = mate;
		}
		for (CramRecord r : newRecords) {
			if (!r.lastSegment && r.next == null)
				r.detached = true;
		}

		time2 = System.nanoTime();
		System.out.println("Container read in " + (time2 - time1) / 1000000
				+ " milli seconds");

		for (int i = 0; i < Math.min(newRecords.size(), 10); i++)
			System.out.println(newRecords.get(i).toString());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		long size = 0;
		for (Slice s : c.slices) {
			size += s.coreBlock.getCompressedContent().length;
			for (Block b : s.external.values()) {
				size += b.getCompressedContent().length;
			}
		}
		System.out.println("Bases: " + baseCount);
		System.out.printf("Uncompressed container size: %d; %.2f\n", size, size
				* 8f / baseCount);
		System.out.printf("Compressed container size: %d; %.2f\n", baos.size(),
				baos.size() * 8f / baseCount);

		File cramFile = new File("c:/temp/test1.cram1");
		FileOutputStream fos = new FileOutputStream(cramFile);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		CramHeader cramHeader = new CramHeader(1, 0, bamFile.getName(),
				samFileReader.getFileHeader());

		ReadWrite.writeCramHeader(cramHeader, bos);
		ReadWrite.writeContainer(c, bos);
		bos.close();
		fos.close();

		time1 = System.nanoTime();
		FileInputStream fis = new FileInputStream(cramFile);
		BufferedInputStream bis = new BufferedInputStream(fis);
		cramHeader = ReadWrite.readCramHeader(bis);
		c = ReadWrite.readContainer(cramHeader.samFileHeader, bis);

		newRecords.clear();
		getRecords(c.h, c, cramHeader.samFileHeader, newRecords);

		mateMap.clear();
		{
			int i = 0;
			for (CramRecord r : newRecords)
				r.index = i++;
		}
		for (CramRecord r : newRecords) {
			if (r.lastSegment) {
				r.recordsToNextFragment = -1;
				if (r.firstSegment)
					continue;
			}

			String name = r.getReadName();
			CramRecord mate = mateMap.get(name);
			if (mate == null) {
				mateMap.put(name, r);
				continue;
			}

			mate.recordsToNextFragment = r.index - mate.index - 1;
			mate.next = r;
			r.previous = mate;
		}
		for (CramRecord r : newRecords) {
			if (!r.lastSegment && r.next == null)
				r.detached = true;
		}

		time2 = System.nanoTime();
		System.out.println("Container read in " + (time2 - time1) / 1000000
				+ " milli seconds");

		for (int i = 0; i < Math.min(newRecords.size(), 10); i++)
			System.out.println(newRecords.get(i).toString());

	}

}
