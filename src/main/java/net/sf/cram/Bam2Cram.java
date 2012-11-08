package net.sf.cram;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.encoding.Writer;
import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.Slice;
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Bam2Cram {

	private static List<CramRecord> convert(List<SAMRecord> samRecords,
			SAMFileHeader samFileHeader, byte[] ref,
			QualityScorePreservation preservation) {

		int sequenceId = samRecords.get(0).getReferenceIndex();
		String sequenceName = samRecords.get(0).getReferenceName();

		int alStart = samRecords.get(0).getAlignmentStart();
		int alEnd = samRecords.get(samRecords.size() - 1).getAlignmentEnd();

		ReferenceTracks tracks = new ReferenceTracks(sequenceId, sequenceName,
				ref, alEnd - alStart + 100);
		tracks.moveForwardTo(alStart);

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(ref);
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		List<CramRecord> cramRecords = new ArrayList<CramRecord>() ;
		int prevAlStart = samRecords.get(0).getAlignmentStart();
		int index = 0;
		for (SAMRecord samRecord : samRecords) {
			if (samRecord.getAlignmentStart() > 0
					&& alStart > samRecord.getAlignmentStart())
				alStart = samRecord.getAlignmentStart();
			if (alEnd < samRecord.getAlignmentEnd())
				alEnd = samRecord.getAlignmentEnd();

			CramRecord cramRecord = f.createCramRecord(samRecord);
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

		return cramRecords;
	}

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out
					.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		if (params.referenceFasta == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		if (params.bamFile == null) {
			System.out.println("A BAM file is required. ");
			System.exit(1);
		}
		
		Log.setGlobalLogLevel(LogLevel.INFO) ;

		File bamFile = params.bamFile;
		SAMFileReader samFileReader = new SAMFileReader(bamFile);
		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(params.referenceFasta);

		ReferenceSequence sequence = null;
		{
			String seqName = null;
			SAMRecordIterator iterator = samFileReader.iterator();
			SAMRecord samRecord = iterator.next();
			seqName = samRecord.getReferenceName();
			iterator.close();
			sequence = referenceSequenceFile.getSequence(seqName);
		}

		List<SAMRecord> samRecords = new ArrayList<SAMRecord>(params.maxContainerSize);
		QualityScorePreservation preservation = new QualityScorePreservation(
				params.qsSpec);

		SAMRecordIterator iterator = samFileReader.iterator();

		int prevSeqId = sequence.getContigIndex();
		byte[] ref = sequence.getBases();
		FileOutputStream fos = new FileOutputStream(params.outputCramFile);
		OutputStream os = new BufferedOutputStream(fos);
		CramHeader h = new CramHeader(1, 0, params.bamFile.getName(),
				samFileReader.getFileHeader());
		ReadWrite.writeCramHeader(h, os);

		long bases = 0;
		long coreBytes = 0;
		long[] externalBytes = new long[10];

		do {
			SAMRecord samRecord = iterator.next();
			if (samRecord.getReferenceIndex() != prevSeqId) {
				if (!samRecords.isEmpty()) {
					List<CramRecord> records = convert(samRecords,
							samFileReader.getFileHeader(), ref, preservation);
					samRecords.clear();
					Container container = BLOCK_PROTO.writeContainer(records,
							samFileReader.getFileHeader(), params.preserveReadNames);
					records.clear();
					ReadWrite.writeContainer(container, os);

					for (Slice s : container.slices) {
						coreBytes += s.coreBlock.compressedContentSize;
						for (Integer i : s.external.keySet())
							externalBytes[i] += s.external.get(i).compressedContentSize;

						s.coreBlock = null;
						s.external.clear();
					}
				}

				if (samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					sequence = referenceSequenceFile.getSequence(samRecord
							.getReferenceName());
					prevSeqId = sequence.getContigIndex();
					ref = sequence.getBases();
				}
			}

			samRecords.add(samRecord);
			bases += samRecord.getReadLength();

			if (samRecords.size() >= params.maxContainerSize) {
				List<CramRecord> records = convert(samRecords,
						samFileReader.getFileHeader(), ref, preservation);
				samRecords.clear();
				Container container = BLOCK_PROTO.writeContainer(records,
						samFileReader.getFileHeader(), params.preserveReadNames);
				records.clear();
				ReadWrite.writeContainer(container, os);
				for (Slice s : container.slices) {
					coreBytes += s.coreBlock.compressedContentSize;
					for (Integer i : s.external.keySet())
						externalBytes[i] += s.external.get(i).compressedContentSize;

					s.coreBlock = null;
					s.external.clear();
				}
			}

			if (params.maxRecords-- < 1)
				break;
		} while (iterator.hasNext());
		iterator.close();
		samFileReader.close();
		
		System.out.printf("STATS: core %.2f b/b", 8f
				* coreBytes / bases);
		for (int i = 0; i < externalBytes.length; i++)
			if (externalBytes[i] > 0)
				System.out.printf(", ex%d %.2f b/b, ", i, 8f
						* externalBytes[i] / bases);
		System.out.println();

		System.out.println(Writer.detachedCount);
	}

	@Parameters(commandDescription = "BAM to CRAM converter. ")
	static class Params {
		@Parameter(names = { "--input-bam-file", "-I" }, converter = FileConverter.class, description = "Path to a BAM file to be converted to CRAM. Omit if standard input (pipe).")
		File bamFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFasta;

		@Parameter(names = { "--output-cram-file", "-O" }, converter = FileConverter.class, description = "The path for the output CRAM file. Omit if standard output (pipe).")
		File outputCramFile = null;

		@Parameter(names = { "--max-records" }, description = "Stop after compressing this many records. ")
		int maxRecords = Integer.MAX_VALUE;

		@Parameter
		List<String> sequences;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--max-slice-size" }, hidden = true)
		int maxSliceSize = 10000;

		@Parameter(names = { "--max-container-size" }, hidden = true)
		int maxContainerSize = 100000;

		// not implemented yet:
		// @Parameter(names = { "--capture-all-tags" }, description =
		// "Capture all tags found in the source BAM file.")
		// boolean captureAllTags = false;
		//
		// @Parameter(names = { "--ignore-tags" }, description =
		// "Do not preserve the tags listed, for example: XA:XO:X0")
		// String ignoreTags;

		@Parameter(names = { "--illumina-quality-score-binning" }, description = "Use NCBI binning scheme for quality scores.")
		boolean illuminaQualityScoreBinning = false;

		@Parameter(names = { "--preserve-read-names" }, description = "Preserve all read names.")
		boolean preserveReadNames = false;

		@Parameter(names = { "--lossy-quality-score-spec", "-L" }, description = "A string specifying what quality scores should be preserved.")
		String qsSpec = "";
	}
}
