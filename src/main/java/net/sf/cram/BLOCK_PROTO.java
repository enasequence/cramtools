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
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.io.DefaultBitOutputStream;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.stats.CompressionHeaderFactory;
import net.sf.cram.structure.Block;
import net.sf.cram.structure.BlockContentType;
import net.sf.cram.structure.CompressionHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.samtools.CigarElement;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;

public class BLOCK_PROTO {
	private static Log log = Log.getInstance(BLOCK_PROTO.class);

	static List<CramRecord> records(CompressionHeader h, Container c,
			SAMFileHeader fileHeader) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		long time1 = System.nanoTime();
		List<CramRecord> records = new ArrayList<>();
		for (Slice s : c.slices)
			records.addAll(records(s, h, fileHeader));

		long time2 = System.nanoTime();

		log.info(String.format("CONTAINER PARSE TIME: %d ms.",(time2 - time1) / 1000000));

		return records;
	}

	private static List<CramRecord> records(Slice s, CompressionHeader h,
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

			try {
				reader.read(r);
			} catch (EOFException e) {
				e.printStackTrace();
				return records;
			}
			records.add(r);

		}

		return records;
	}

	static Container writeContainer(List<CramRecord> records,
			SAMFileHeader fileHeader) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		// get stats, create compression header and slices
		long time1 = System.nanoTime();
		CompressionHeader h = new CompressionHeaderFactory().build(records);
		long time2 = System.nanoTime();

		h.mappedQualityScoreIncluded = true;
		h.unmappedQualityScoreIncluded = true;
		h.readNamesIncluded = true;

		int recordsPerSlice = 10000;

		List<Slice> slices = new ArrayList<>();

		Container c = new Container();
		c.h = h;
		c.nofRecords = records.size();

		long time3 = System.nanoTime();
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
		long time4 = System.nanoTime();

		c.slices = (Slice[]) slices.toArray(new Slice[slices.size()]);

		log.info(String
				.format("CONTAINER BUILD TIME: header %d ms, slices %d ms, total %d ms.",
						(time2 - time1) / 1000000, (time4 - time3) / 1000000,
						(time4 - time1) / 1000000));
		return c;
	}

	private static Slice writeSlice(List<CramRecord> records,
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
		slice.coreBlock.contentType = BlockContentType.CORE;
		bos.close();

		slice.external = new HashMap<>();
		for (Integer i : map.keySet()) {
			ExposedByteArrayOutputStream os = map.get(i);

			Block externalBlock = new Block();
			externalBlock.contentType = BlockContentType.EXTERNAL;
			externalBlock.contentId = i;
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
		List<SAMRecord> samRecords = new ArrayList<>(maxRecords);

		int alStart = Integer.MAX_VALUE;
		int alEnd = 0;
		long baseCount = 0;
		SAMRecordIterator iterator = samFileReader.iterator();
		// while (!"SRR081241.20878257".equals(iterator.next().getReadName()))
		// ;

		do {
			SAMRecord samRecord = iterator.next();
			// if (!"SRR081241.20758946".equals(samRecord.getReadName()))
			// continue;
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

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(sequence.getBases());
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		List<CramRecord> cramRecords = new ArrayList<>(maxRecords);
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
		Container c = writeContainer(cramRecords, samFileReader.getFileHeader());
		long time2 = System.nanoTime();
		System.out.println("Container written in " + (time2 - time1) / 1000000
				+ " milli seconds");

		time1 = System.nanoTime();
		List<CramRecord> newRecords = records(c.h, c,
				samFileReader.getFileHeader());

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

		newRecords = records(c.h, c, cramHeader.samFileHeader);

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
