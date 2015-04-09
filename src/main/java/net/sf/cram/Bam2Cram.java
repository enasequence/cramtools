/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.FixBAMFileHeader.MD5MismatchError;
import net.sf.cram.build.ContainerFactory;
import net.sf.cram.build.CramIO;
import net.sf.cram.build.Sam2CramRecordFactory;
import net.sf.cram.common.Utils;
import net.sf.cram.lossy.QualityScorePreservation;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.ref.ReferenceTracks;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.CigarElement;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import cipheronly.CipherOutputStream_256;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Bam2Cram {
	private static Log log = Log.getInstance(Bam2Cram.class);
	public static final String COMMAND = "cram";

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

	public static void updateTracks(List<SAMRecord> samRecords,
			ReferenceTracks tracks) {
		for (SAMRecord samRecord : samRecords) {
			if (samRecord.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START
					&& samRecord.getReadBases().length > 0) {
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
		}
	}

	public static List<CramRecord> convert(List<SAMRecord> samRecords,
			SAMFileHeader samFileHeader, byte[] ref, ReferenceTracks tracks,
			QualityScorePreservation preservation, boolean captureAllTags,
			String captureTags, String ignoreTags) {

		int sequenceId = samRecords.get(0).getReferenceIndex();
		String sequenceName = samRecords.get(0).getReferenceName();

		log.debug(String.format("Writing %d records for sequence %d, %s",
				samRecords.size(), sequenceId, sequenceName));

		Sam2CramRecordFactory f = new Sam2CramRecordFactory(ref, samFileHeader);
		f.captureUnmappedBases = true;
		f.captureUnmappedScores = true;
		f.captureAllTags = captureAllTags;
		f.captureTags = tagsNamesToSet(captureTags);
		f.ignoreTags.addAll(tagsNamesToSet(ignoreTags));

		List<CramRecord> cramRecords = new ArrayList<CramRecord>();
		int prevAlStart = samRecords.get(0).getAlignmentStart();
		int index = 0;

		long tracksNanos = System.nanoTime();
		updateTracks(samRecords, tracks);
		tracksNanos = System.nanoTime() - tracksNanos;

		long createNanos = System.nanoTime();
		for (SAMRecord samRecord : samRecords) {
			CramRecord cramRecord = f.createCramRecord(samRecord);
			cramRecord.index = ++index;
			cramRecord.alignmentDelta = samRecord.getAlignmentStart()
					- prevAlStart;
			cramRecord.alignmentStart = samRecord.getAlignmentStart();
			prevAlStart = samRecord.getAlignmentStart();

			cramRecords.add(cramRecord);

			preservation.addQualityScores(samRecord, cramRecord, tracks);
		}

		if (f.getBaseCount() < 3 * f.getFeatureCount())
			log.warn("Abnormally high number of mismatches, possibly wrong reference.");

		createNanos = System.nanoTime() - createNanos;

		// mating:
		long mateNanos = System.nanoTime();
		Map<String, CramRecord> mateMap = new TreeMap<String, CramRecord>();
		for (CramRecord r : cramRecords) {
			if (!r.isMultiFragment()) {
				r.setDetached(true);

				r.setHasMateDownStream(false);
				r.recordsToNextFragment = -1;
				r.next = null;
				r.previous = null;
			} else {
				String name = r.readName;
				CramRecord mate = mateMap.get(name);
				if (mate == null) {
					mateMap.put(name, r);
				} else {
					mate.recordsToNextFragment = r.index - mate.index - 1;
					mate.next = r;
					r.previous = mate;
					r.previous.setHasMateDownStream(true);
					r.setHasMateDownStream(false);
					r.setDetached(false);
					r.previous.setDetached(false);

					mateMap.remove(name);
				}
			}
		}

		for (CramRecord r : mateMap.values()) {
			r.setDetached(true);

			r.setHasMateDownStream(false);
			r.recordsToNextFragment = -1;
			r.next = null;
			r.previous = null;
		}
		mateNanos = System.nanoTime() - mateNanos;
		log.info(String.format(
				"create: tracks %dms, records %dms, mating %dms.",
				tracksNanos / 1000000, createNanos / 1000000,
				mateNanos / 1000000));

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
			IllegalArgumentException, IllegalAccessException,
			NoSuchAlgorithmException {
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

		Log.setGlobalLogLevel(params.logLevel);

		if (params.referenceFasta == null)
			log.warn("No reference file specified, remote access over internet may be used to download public sequences. ");
		ReferenceSource referenceSource = new ReferenceSource(
				params.referenceFasta);

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
		SAMFileHeader samFileHeader = samFileReader.getFileHeader().clone();

		SAMSequenceRecord samSequenceRecord = null;
		List<SAMRecord> samRecords = new ArrayList<SAMRecord>(
				params.maxSliceSize);
		int prevSeqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
		SAMRecordIterator iterator = samFileReader.iterator();
		SAMRecord samRecord;
		{
			String seqName = null;
			samRecord = iterator.next();
			if (samRecord == null)
				throw new RuntimeException("No records found.");
			seqName = samRecord.getReferenceName();
			prevSeqId = samRecord.getReferenceIndex();
			samRecords.add(samRecord);

			if (SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(seqName))
				samSequenceRecord = null;
			else
				samSequenceRecord = samFileHeader.getSequence(seqName);
		}

		QualityScorePreservation preservation;
		if (params.losslessQS)
			preservation = new QualityScorePreservation("*40");
		else
			preservation = new QualityScorePreservation(params.qsSpec);

		byte[] ref = null;
		ReferenceTracks tracks = null;
		if (samSequenceRecord == null) {
			ref = new byte[0];
			tracks = new ReferenceTracks(
					SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX,
					SAMRecord.NO_ALIGNMENT_REFERENCE_NAME, ref);
		} else {
			ref = referenceSource.getReferenceBases(samSequenceRecord, true);
			log.debug(String.format(
					"Creating tracks for reference: name=%s, length=%d.\n",
					samSequenceRecord.getSequenceName(), ref.length));
			tracks = new ReferenceTracks(samSequenceRecord.getSequenceIndex(),
					samSequenceRecord.getSequenceName(), ref);
		}

		OutputStream os;
		if (params.outputCramFile != null) {
			FileOutputStream fos = new FileOutputStream(params.outputCramFile);
			os = new BufferedOutputStream(fos);
		} else {
			log.warn("No output file, writint to STDOUT.");
			os = System.out;
		}

		if (params.encrypt) {
			CipherOutputStream_256 cos = new CipherOutputStream_256(os, pass,
					128);
			os = cos.getCipherOutputStream();
		}

		FixBAMFileHeader fixBAMFileHeader = new FixBAMFileHeader(
				referenceSource);
		fixBAMFileHeader.setConfirmMD5(params.confirmMD5);
		fixBAMFileHeader.setInjectURI(params.injectURI);
		fixBAMFileHeader.setIgnoreMD5Mismatch(params.ignoreMD5Mismatch);
		try {
			fixBAMFileHeader.fixSequences(samFileHeader.getSequenceDictionary()
					.getSequences());
		} catch (MD5MismatchError e) {
			log.error(e.getMessage());
			System.exit(1);
		}
		fixBAMFileHeader.addCramtoolsPG(samFileHeader);

		CramHeader h = new CramHeader(Utils.getMajorVersion(),
				Utils.getMinorVersion(), params.bamFile == null ? "STDIN"
						: params.bamFile.getName(), samFileHeader);
		long offset = CramIO.writeCramHeader(h, os);

		long bases = 0;
		long coreBytes = 0;
		Map<Integer, Long> externalBytes = new HashMap<Integer, Long>();

		ContainerFactory cf = new ContainerFactory(samFileHeader,
				params.maxContainerSize, params.preserveReadNames);

		do {
			if (params.outputCramFile == null && System.out.checkError())
				return;

			samRecord = iterator.hasNext() ? iterator.next() : null;
			if (samRecord == null || samRecord.getReferenceIndex() != prevSeqId
					|| samRecords.size() >= params.maxContainerSize) {
				if (!samRecords.isEmpty()) {
					long convertNanos = 0;
					convertNanos = System.nanoTime();

					List<CramRecord> records = convert(samRecords,
							samFileHeader, ref, tracks, preservation,
							params.captureAllTags, params.captureTags,
							params.ignoreTags);
					convertNanos = System.nanoTime() - convertNanos;
					samRecords.clear();

					Container container = cf.buildContainer(records);

					for (Slice s : container.slices)
						s.setRefMD5(ref);

					records.clear();
					long len = CramIO.writeContainer(container, os);
					container.offset = offset;
					offset += len;

					log.info(String
							.format("CONTAINER WRITE TIMES: records build time %dms, header build time %dms, slices build time %dms, io time %dms.",
									convertNanos / 1000000,
									container.buildHeaderTime / 1000000,
									container.buildSlicesTime / 1000000,
									container.writeTime / 1000000));

					for (Slice s : container.slices) {
						coreBytes += s.coreBlock.getCompressedContent().length;
						for (Integer i : s.external.keySet()) {
							if (!externalBytes.containsKey(i))
								externalBytes.put(i, 0L);
							long value = externalBytes.get(i)
									+ s.external.get(i).getCompressedContent().length;
							externalBytes.put(i, value);
						}
					}
				}
			}
			if (samRecord == null)
				break;

			if (prevSeqId != samRecord.getReferenceIndex()) {
				if (samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					samSequenceRecord = samFileHeader.getSequence(samRecord
							.getReferenceName());
					ref = referenceSource.getReferenceBases(samSequenceRecord,
							true);
					tracks = new ReferenceTracks(
							samSequenceRecord.getSequenceIndex(),
							samSequenceRecord.getSequenceName(), ref);

				} else {
					ref = new byte[] {};
					tracks = new ReferenceTracks(
							SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX,
							SAMRecord.NO_ALIGNMENT_REFERENCE_NAME, ref);
				}
				prevSeqId = samRecord.getReferenceIndex();
			}

			samRecords.add(samRecord);
			bases += samRecord.getReadLength();

			if (params.maxRecords-- < 1)
				break;
		} while (samRecord != null && !samRecords.isEmpty());

		iterator.close();
		samFileReader.close();
		if (params.addEOF)
			CramIO.issueZeroF_EOF_marker(os);
		os.close();

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("STATS: core %.2f b/b", 8f * coreBytes / bases));
		for (int i : externalBytes.keySet())
			if (externalBytes.get(i) > 0)
				sb.append(String.format(", ex%d %.2f b/b, ", i, 8f
						* externalBytes.get(i) / bases));

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
		long maxRecords = Long.MAX_VALUE;

		@Parameter
		List<String> sequences;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--max-slice-size" }, hidden = true)
		int maxSliceSize = 10000;

		@Parameter(names = { "--max-container-size" }, hidden = true)
		int maxContainerSize = 10000;

		@Parameter(names = { "--preserve-read-names", "-n" }, description = "Preserve all read names.")
		boolean preserveReadNames = false;

		@Parameter(names = { "--lossless-quality-score", "-Q" }, description = "Preserve all quality scores. Overwrites '--lossless-quality-score'.")
		boolean losslessQS = false;

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

		@Parameter(names = { "--inject-sq-uri" }, description = "Inject or change the @SQ:UR header fields to point to ENA reference service. ")
		public boolean injectURI = false;

		@Parameter(names = { "--ignore-md5-mismatch" }, description = "Fail on MD5 mismatch if true, or correct (overwrite) the checksums and continue if false.")
		public boolean ignoreMD5Mismatch = false;

		@Deprecated
		@Parameter(names = { "--issue-eof-marker" }, description = "Append the EOF marker to the end of the output file/stream.", hidden = true, arity = 1)
		public boolean addEOF = true;

		@Parameter(names = { "--confirm-md5" }, description = "Confirm MD5 checksums of the reference sequences.", hidden = true, arity = 1)
		public boolean confirmMD5 = true;
	}
}
