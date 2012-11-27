package net.sf.cram;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.ReadWrite.CramHeader;
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
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import uk.ac.ebi.embl.ega_cipher.CipherOutputStream_256;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Bam2Cram {
	private static Log log = Log.getInstance(Bam2Cram.class);

	private static Set<String> tagsNamesToSet(String tags) {
		Set<String> set = new TreeSet<String>();
		if (tags == null || tags.length() == 0)
			return set;

		String[] chunks = tags.split(":");
		for (String s : chunks) {
			if (s.length() != 2)
				throw new RuntimeException(
						"Expecting column delimited tags names but got: '"
								+ tags + "'");
			set.add(s);
		}
		return set;
	}

	private static List<CramRecord> convert(List<SAMRecord> samRecords,
			SAMFileHeader samFileHeader, byte[] ref,
			QualityScorePreservation preservation, boolean captureAllTags,
			String captureTags, String ignoreTags) {

		int sequenceId = samRecords.get(0).getReferenceIndex();
		String sequenceName = samRecords.get(0).getReferenceName();

		log.debug(String.format("Writing %d records for sequence %d, %s",
				samRecords.size(), sequenceId, sequenceName));

		int alStart = Integer.MAX_VALUE;
		int alEnd = Integer.MIN_VALUE;
		for (SAMRecord samRecord : samRecords) {
			int start = samRecord.getAlignmentStart();
			if (start != SAMRecord.NO_ALIGNMENT_START)
				alStart = alStart > start ? start : alStart;

			int end = samRecord.getAlignmentEnd();
			if (end != SAMRecord.NO_ALIGNMENT_START)
				alEnd = alEnd < end ? end : alEnd;
		}

		log.debug("Reads start at " + alStart + ", stop at " + alEnd);
		if (alEnd < alStart)
			alEnd = alStart + 1000;

		ReferenceTracks tracks = null;
		if (alStart < Integer.MAX_VALUE
				&& alStart != SAMRecord.NO_ALIGNMENT_START) {
			tracks = new ReferenceTracks(sequenceId, sequenceName, ref, 1000);
			tracks.moveForwardTo(alStart);
		}

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(ref, samFileHeader);
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		f.captureAllTags = captureAllTags;
		f.captureTags = tagsNamesToSet(captureTags);
		f.ignoreTags.addAll(tagsNamesToSet(captureTags));

		List<CramRecord> cramRecords = new ArrayList<CramRecord>();
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
			if (samRecord.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START) {
				tracks.ensure(samRecord.getAlignmentStart(),
						samRecord.getAlignmentStart());
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
							byte readBase = samRecord.getReadBases()[readPos
									+ i];
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

		Log.setGlobalLogLevel(params.logLevel);

		char[] pass = null;
		if (params.encrypt) {
			if (System.console() == null)
				throw new RuntimeException("Cannot access console.");
			pass = System.console().readPassword();
		}

		File bamFile = params.bamFile;
		SAMFileReader
				.setDefaultValidationStringency(ValidationStringency.SILENT);

		SAMFileReader samFileReader = null;
		if (params.bamFile == null) {
			log.warn("No input file, reading from input...");
			samFileReader = new SAMFileReader(System.in);
		} else
			samFileReader = new SAMFileReader(bamFile);

		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory
				.getReferenceSequenceFile(params.referenceFasta);

		ReferenceSequence sequence = null;
		List<SAMRecord> samRecords = new ArrayList<SAMRecord>(
				params.maxContainerSize);
		int prevSeqId = -1;
		SAMRecordIterator iterator = samFileReader.iterator();
		{
			String seqName = null;
			SAMRecord samRecord = iterator.next();
			samRecords.add(samRecord);
			seqName = samRecord.getReferenceName();
			prevSeqId = samRecord.getReferenceIndex() ;

			if (samFileReader.getFileHeader().getReadGroups().isEmpty()
					|| samFileReader.getFileHeader().getReadGroup(
							Sam2CramRecordFactory.UNKNOWN_READ_GROUP_ID) == null) {
				log.info("Adding default read group.");
				SAMReadGroupRecord readGroup = new SAMReadGroupRecord(
						Sam2CramRecordFactory.UNKNOWN_READ_GROUP_ID);
				readGroup
						.setSample(Sam2CramRecordFactory.UNKNOWN_READ_GROUP_SAMPLE);
				samFileReader.getFileHeader().addReadGroup(readGroup);
			}

			sequence = referenceSequenceFile.getSequence(seqName);
		}

		QualityScorePreservation preservation = new QualityScorePreservation(
				params.qsSpec);


		byte[] ref = sequence.getBases();

		OutputStream os;
		Index index = null;
		if (params.outputCramFile != null) {
			FileOutputStream fos = new FileOutputStream(params.outputCramFile);
			os = new BufferedOutputStream(fos);
			index = new Index(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(
					new File(params.outputCramFile + ".crai")))));
		} else {
			log.warn("No output file, writint to STDOUT.");
			os = System.out;
		}

		if (params.encrypt) {
			CipherOutputStream_256 cos = new CipherOutputStream_256(os, pass,
					128);
			os = cos.getCipherOutputStream();
		}

		CramHeader h = new CramHeader(1, 0, params.bamFile == null ? "STDIN"
				: params.bamFile.getName(), samFileReader.getFileHeader());
		long offset = ReadWrite.writeCramHeader(h, os);

		long bases = 0;
		long coreBytes = 0;
		long[] externalBytes = new long[10];
		BLOCK_PROTO.recordsPerSlice = params.maxSliceSize;

		do {
			if (params.outputCramFile == null && System.out.checkError())
				return;

			SAMRecord samRecord = iterator.next();
			if (samRecord.getReferenceIndex() != prevSeqId
					|| samRecords.size() >= params.maxContainerSize) {
				if (!samRecords.isEmpty()) {
					List<CramRecord> records = convert(samRecords,
							samFileReader.getFileHeader(), ref, preservation,
							params.captureAllTags, params.captureTags,
							params.ignoreTags);
					samRecords.clear();

					Container container = BLOCK_PROTO.buildContainer(records,
							samFileReader.getFileHeader(),
							params.preserveReadNames);
					records.clear();
					long len = ReadWrite.writeContainer(container, os);
					if (index != null)
						index.addContainer(container, offset);
					offset += len;

					log.info(String
							.format("CONTAINER WRITE TIMES: header build time %dms, slices build time %dms, io time %dms.",
									container.buildHeaderTime / 1000000,
									container.buildSlicesTime / 1000000,
									container.writeTime / 1000000));

					for (Slice s : container.slices) {
						coreBytes += s.coreBlock.compressedContentSize;
						for (Integer i : s.external.keySet())
							externalBytes[i] += s.external.get(i).compressedContentSize;
					}
				}

				if (samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					sequence = referenceSequenceFile.getSequence(samRecord
							.getReferenceName());
					ref = sequence.getBases();
				}
				prevSeqId = samRecord.getReferenceIndex();
			}

			samRecords.add(samRecord);
			bases += samRecord.getReadLength();

			if (params.maxRecords-- < 1)
				break;
		} while (iterator.hasNext());

		{ // copied for now, should be a subroutine:
			if (!samRecords.isEmpty()) {
				List<CramRecord> records = convert(samRecords,
						samFileReader.getFileHeader(), ref, preservation,
						params.captureAllTags, params.captureTags,
						params.ignoreTags);
				samRecords.clear();
				Container container = BLOCK_PROTO
						.buildContainer(records, samFileReader.getFileHeader(),
								params.preserveReadNames);
				records.clear();
				ReadWrite.writeContainer(container, os);
				log.info(String
						.format("CONTAINER WRITE TIMES: header build time %dms, slices build time %dms, io time %dms.",
								container.buildHeaderTime / 1000000,
								container.buildSlicesTime / 1000000,
								container.writeTime / 1000000));

				for (Slice s : container.slices) {
					coreBytes += s.coreBlock.compressedContentSize;
					for (Integer i : s.external.keySet())
						externalBytes[i] += s.external.get(i).compressedContentSize;
				}
			}
		}

		iterator.close();
		samFileReader.close();
		os.close();
		index.close() ;

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("STATS: core %.2f b/b", 8f * coreBytes / bases));
		for (int i = 0; i < externalBytes.length; i++)
			if (externalBytes[i] > 0)
				sb.append(String.format(", ex%d %.2f b/b, ", i, 8f
						* externalBytes[i] / bases));

		log.info(sb.toString());
		if (params.outputCramFile != null)
			log.info(String.format("Compression: %.2f b/b.",
					(8f * params.outputCramFile.length() / bases)));
	}

	@Parameters(commandDescription = "BAM to CRAM converter. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

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

		@Parameter(names = { "--illumina-quality-score-binning" }, description = "Use NCBI binning scheme for quality scores.")
		boolean illuminaQualityScoreBinning = false;

		@Parameter(names = { "--preserve-read-names" }, description = "Preserve all read names.")
		boolean preserveReadNames = false;

		@Parameter(names = { "--lossy-quality-score-spec", "-L" }, description = "A string specifying what quality scores should be preserved.")
		String qsSpec = "";

		@Parameter(names = { "--encrypt" }, description = "Encrypt the CRAM file.")
		boolean encrypt = false;

		@Parameter(names = { "--ignore-tags" }, description = "Ignore the tags listed, for example 'OQ:XA:XB'")
		String ignoreTags = "";

		@Parameter(names = { "--capture-tags" }, description = "Capture the tags listed, for example 'OQ:XA:XB'")
		String captureTags = "";

		@Parameter(names = { "--capture-all-tags" }, description = "Capture all tags.")
		boolean captureAllTags = false;

		@Parameter(names = { "--input-is-sam" }, description = "Input is in SAM format.")
		boolean inputIsSam = false;
	}
}
