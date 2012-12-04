package net.sf.cram;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.encoding.Reader;
import net.sf.cram.encoding.Writer;
import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.structure.Container;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.CigarElement;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;

public class SCSTest {

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
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
		Map<String, List<SAMRecord>> origSamMap = new TreeMap<String, List<SAMRecord>>();

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
			List<SAMRecord> list = origSamMap.get(samRecord.getReadName());
			if (list == null) {
				list = new ArrayList<SAMRecord>(2);
				origSamMap.put(samRecord.getReadName(), list);
			}
			list.add(samRecord);

			if (samRecord.getAlignmentStart() > 0
					&& alStart > samRecord.getAlignmentStart())
				alStart = samRecord.getAlignmentStart();
			if (alEnd < samRecord.getAlignmentEnd())
				alEnd = samRecord.getAlignmentEnd();

			if (samRecords.size() >= maxRecords)
				break;
		} while (iterator.hasNext());

		byte[] ref = sequence.getBases();
		ReferenceTracks tracks = new ReferenceTracks(sequence.getContigIndex(),
				sequence.getName(), ref, alEnd - alStart + 100);
		tracks.moveForwardTo(alStart);

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(
				sequence.getBases(), samFileReader.getFileHeader());
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		ArrayList<CramRecord> cramRecords = new ArrayList<CramRecord>(
				maxRecords);
		int prevAlStart = samRecords.get(0).getAlignmentStart();
		int index = 0;
		QualityScorePreservation preservation = new QualityScorePreservation(
				"M40");
		for (SAMRecord samRecord : samRecords) {

			CramRecord cramRecord = f.createCramRecord(samRecord);
			// if ("SRR077487.23914624".equals(samRecord.getReadName())) {
			// System.err.println("SAM scores: " +
			// Arrays.toString(samRecord.getBaseQualities()));
			// System.err.println("CRAM scores: " +
			// Arrays.toString(cramRecord.getQualityScores()));
			// }
			cramRecord.index = ++index;
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
			String name = r.getReadName();
			CramRecord mate = mateMap.get(name);
			if (mate == null) {
				mateMap.put(name, r);
			} else {
				mate.recordsToNextFragment = r.index - mate.index - 1;
				mate.next = r;
				r.previous = mate;
				r.previous.hasMateDownStream = true;
				r.hasMateDownStream = false;
				r.detached = false;
				r.previous.detached = false;

				mateMap.remove(name);
			}
		}

		for (CramRecord r : mateMap.values()) {
			r.detached = true;

			r.hasMateDownStream = false;
			r.recordsToNextFragment = -1;
			r.next = null;
			r.previous = null;
		}

		List<CramRecord> old = cramRecords;
		Container c = BLOCK_PROTO.buildContainer(cramRecords,
				samFileReader.getFileHeader(), true);
		System.err.println("Written " + Writer.detachedCount
				+ " detached records.");

		try {
			cramRecords.clear();
			BLOCK_PROTO.getRecords(c.h, c, samFileReader.getFileHeader(),
					cramRecords);
		} catch (Exception e1) {
			System.err.println("Read " + Reader.detachedCount
					+ " detached records.");
			throw new RuntimeException(e1);
		}

		for (int i = 0; i < cramRecords.size(); i++) {
			if (!old.get(i).getReadName()
					.equals(cramRecords.get(i).getReadName())) {
				System.err.println("Read name mismatch");
				System.err.println(old.get(i).toString());
				System.err.println(cramRecords.get(i).toString());
				break;
			}
			// if (old.get(i).detached != cramRecords.get(i).detached)
			// System.err.println(cramRecords.get(i).toString());
		}

		if (true)
			return;

		long time1 = System.nanoTime();
		CramNormalizer n = new CramNormalizer(samFileReader.getFileHeader());
		n.normalize(cramRecords, true, ref, alStart);
		long time2 = System.nanoTime();
		System.err.printf("Normalized in %d ms.\n", (time2 - time1) / 1000000);

		time1 = System.nanoTime();
		Cram2BamRecordFactory c2sFactory = new Cram2BamRecordFactory(
				samFileReader.getFileHeader());

		List<SAMRecord> newSAMRecords = new ArrayList<SAMRecord>();
		Map<String, List<SAMRecord>> newSamMap = new TreeMap<String, List<SAMRecord>>();
		for (CramRecord r : cramRecords) {
			SAMRecord s = c2sFactory.create(r);
			newSAMRecords.add(s);

			List<SAMRecord> list = newSamMap.get(s.getReadName());
			if (list == null) {
				list = new ArrayList<SAMRecord>(2);
				newSamMap.put(s.getReadName(), list);
			}
			list.add(s);
		}

		time2 = System.nanoTime();
		System.err.printf("Converted to SAM in %d ms.\n",
				(time2 - time1) / 1000000);

		int maxErrors = 10;
		boolean skipQS = true;
		for (int i = 0; i < newSAMRecords.size(); i++) {
			if (maxErrors < 1) {
				System.err.println("Too many errors, breaking.");
				break;
			}

			SAMRecord r1 = samRecords.get(i);
			SAMRecord r2 = newSAMRecords.get(i);

			if (!r1.getReadName().equals(r2.getReadName())) {
				System.err.printf("NAME: %s %s\n", r1.getReadName(),
						r2.getReadName());
				maxErrors--;
				continue;
			}

			if (r1.getFlags() != r2.getFlags()) {
				System.err.printf("FLAGS %s: %d %d\n", r1.getReadName(),
						r1.getFlags(), r2.getFlags());
				for (SAMRecord r : origSamMap.get(r1.getReadName()))
					System.err.print("ORIG: " + r.getSAMString());

				for (SAMRecord r : newSamMap.get(r1.getReadName()))
					System.err.print("NEW: " + r.getSAMString());

				maxErrors--;
				continue;
			}

			if (!r1.getReferenceName().equals(r2.getReferenceName())) {
				System.err.printf("REF %s: %s %s\n", r1.getReadName(),
						r1.getReferenceName(), r2.getReferenceName());

				maxErrors--;
				continue;
			}

			if (r1.getAlignmentStart() != r2.getAlignmentStart()) {
				System.err.printf("START %s: %d %d\n", r1.getReadName(),
						r1.getAlignmentStart(), r2.getAlignmentStart());

				maxErrors--;
				continue;
			}

			if (r1.getMappingQuality() != r2.getMappingQuality()) {
				System.err.printf("MSCORE %s: %d %d\n", r1.getReadName(),
						r1.getMappingQuality(), r2.getMappingQuality());

				maxErrors--;
				continue;
			}

			if (!r1.getCigar().equals(r2.getCigar())) {
				System.err.printf("CIAGR %s: %s %s\n", r1.getReadName(),
						r1.getCigarString(), r2.getCigarString());

				maxErrors--;
				continue;
			}

			if (!r1.getMateReferenceName().equals(r2.getMateReferenceName())) {
				System.err.printf("MREF %s: %s %s\n", r1.getReadName(),
						r1.getMateReferenceName(), r2.getMateReferenceName());

				maxErrors--;
				continue;
			}

			if (r1.getMateAlignmentStart() != r2.getMateAlignmentStart()) {
				System.err.printf("MSTART %s: %d %d\n", r1.getReadName(),
						r1.getMateAlignmentStart(), r2.getMateAlignmentStart());

				maxErrors--;
				continue;
			}

			if (r1.getInferredInsertSize() != r2.getInferredInsertSize()) {
				System.err.printf("ISIZE %s: %d %d\n", r1.getReadName(),
						r1.getInferredInsertSize(), r2.getInferredInsertSize());

				maxErrors--;
				continue;
			}

			if (!Arrays.equals(r1.getReadBases(), r2.getReadBases())) {
				System.err.printf("BASES %s: %s %s\n", r1.getReadName(),
						new String(r1.getReadBases()),
						new String(r2.getReadBases()));

				maxErrors--;
				continue;
			}

			if (!skipQS)
				try {
					if (!r1.getBaseQualityString().equals(
							r2.getBaseQualityString())) {
						System.err.printf("SCORES %s: %s %s\n",
								r1.getReadName(), r1.getBaseQualityString(),
								r2.getBaseQualityString());

						maxErrors--;
						continue;
					}
				} catch (IllegalArgumentException e) {
					System.err
							.println("Intercepted exception for the following records:");
					System.err.print(r1.getSAMString());
					System.err.print(r2.getSAMString());
					throw e;
				}
		}
	}
}
