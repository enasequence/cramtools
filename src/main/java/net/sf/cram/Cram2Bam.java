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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import htsjdk.samtools.CRAMFileReader;
import htsjdk.samtools.Defaults;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.ContainerParser;
import htsjdk.samtools.cram.build.Cram2SamRecordFactory;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.build.CramNormalizer;
import htsjdk.samtools.cram.structure.*;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.Log;
import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.FixBAMFileHeader.MD5MismatchError;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import net.sf.cram.common.Utils;
import net.sf.cram.index.CramIndex;
import net.sf.cram.ref.ReferenceSource;
import htsjdk.samtools.BAMIndexFactory;

public class Cram2Bam {
	private static Log log = Log.getInstance(Cram2Bam.class);
	public static final String COMMAND = "bam";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Cram2Bam.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameteres, detailed message below: ");
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

		if (params.reference == null)
			log.warn("No reference file specified, remote access over internet may be used to download public sequences. ");

		if (params.locations == null)
			params.locations = new ArrayList<String>();

		InputStream is = null;
		try {
			is = Utils.openCramInputStream(params.cramURL, params.decrypt, params.password);
		} catch (Exception e2) {
			log.error("Failed to open CRAM from: " + params.cramURL, e2);
			System.exit(1);
		}

		CramHeader cramHeader = CramIO.readCramHeader(is);

		if (params.printSAMHeaderOnly) {
			System.out.println(cramHeader.getSamFileHeader().getTextHeader());
			return;
		}

		ReferenceSource referenceSource = new ReferenceSource(params.reference);
		referenceSource.setDownloadTriesBeforeFailing(params.downloadTriesBeforeFailing);

		FixBAMFileHeader fix = new FixBAMFileHeader(referenceSource);
		fix.setConfirmMD5(!params.skipMD5Checks);
		fix.setInjectURI(params.injectURI);
		fix.setIgnoreMD5Mismatch(params.ignoreMD5Mismatch);
		try {
			log.info("Preparing the header...");
			fix.fixSequences(cramHeader.getSamFileHeader().getSequenceDictionary().getSequences());
		} catch (MD5MismatchError e) {
			log.error(e.getMessage());
			System.exit(1);
		}
		fix.addCramtoolsPG(cramHeader.getSamFileHeader());

		BlockCompressedOutputStream.setDefaultCompressionLevel(Defaults.COMPRESSION_LEVEL);
		SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();
		samFileWriterFactory.setAsyncOutputBufferSize(params.asyncBamBuffer);
		samFileWriterFactory.setCreateIndex(false);
		samFileWriterFactory.setCreateMd5File(false);
		samFileWriterFactory.setUseAsyncIo(params.syncBamOutput);

		SAMFileWriter writer = createSAMFileWriter(params, cramHeader, samFileWriterFactory);

		htsjdk.samtools.cram.structure.Container c = null;
		AlignmentSliceQuery location = null;
		if (!params.locations.isEmpty()) {
			if (params.locations.size() > 1)
				throw new RuntimeException("Only one location is supported.");
			if (!(is instanceof SeekableStream))
				throw new RuntimeException("Cannot use random access on a stream.");

			location = new AlignmentSliceQuery(params.locations.get(0));
			location.sequenceId = cramHeader.getSamFileHeader().getSequenceIndex(location.sequence);
			if (location.sequenceId < 0) {
				log.error("Reference sequence not found for name: " + location.sequence);
				return;
			}

			try {
				log.info("Seeking for the query " + location.toString());
				c = skipToContainer((SeekableStream) is, cramHeader, location);
			} catch (ReadNotFoundException e) {
				log.warn("Nothing found for query " + location);
				return;
			} catch (URISyntaxException e) {
				log.warn("Failed to fetch or parse: " + location, e);
				return;
			}

			// if (c == null) {
			// log.error("Index file not found. ");
			// return;
			// }
		}

//		CRAMFileReader cramFileReader = new CRAMFileReader(new FileInputStream(params.cramURL), null, referenceSource, ValidationStringency.SILENT);
//		final SAMRecordIterator iterator = cramFileReader.getIterator();
//		while (iterator.hasNext()) {
//			writer.addAlignment(iterator.next());
//		}
//		iterator.close();
//		writer.close();
//		if(true)
//			return;

		long recordCount = 0;
		long baseCount = 0;
		long readTime = 0;
		long parseTime = 0;
		long normTime = 0;
		long samTime = 0;
		long writeTime = 0;
		long time = 0;
		ArrayList<CramCompressionRecord> cramRecords = new ArrayList<CramCompressionRecord>(10000);

		CramNormalizer n = new CramNormalizer(cramHeader.getSamFileHeader(), referenceSource);

		byte[] ref = null;
		int prevSeqId = -1;

		ContainerParser parser = new ContainerParser(cramHeader.getSamFileHeader());
		while (true) {
			if (params.maxContainers-- <= 0)
				break;

			time = System.nanoTime();
			c = ContainerIO.readContainer(cramHeader.getVersion(), is);
			if (c.isEOF())
				break;

			readTime += System.nanoTime() - time;

			// for random access check if the sequence is the one we are looking
			// for:
			if (location != null && location.sequenceId != c.sequenceId)
				break;

			if (params.countOnly && location == null && params.requiredFlags == 0 && params.filteringFlags == 0) {
				recordCount += c.nofRecords;
				baseCount += c.bases;
				continue;
			}

				time = System.nanoTime();
				cramRecords.clear();
				parser.getRecords(c, cramRecords);
				parseTime += System.nanoTime() - time;

			switch (c.sequenceId) {
			case SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX:
			case -2:
				ref = new byte[] {};
				break;

			default:
				if (prevSeqId < 0 || prevSeqId != c.sequenceId) {
					SAMSequenceRecord sequence = cramHeader.getSamFileHeader().getSequence(c.sequenceId);
					log.info("Loading reference sequence " + sequence.getSequenceName());
					ref = referenceSource.getReferenceBases(sequence, true);
					Utils.upperCase(ref);
					prevSeqId = c.sequenceId;
				}
				break;
			}

				for (int i = 0; i < c.slices.length; i++) {
					Slice s = c.slices[i];
					if (s.sequenceId < 0)
						continue;
					if (!s.validateRefMD5(ref)) {
						log.error(String
								.format("Reference sequence MD5 mismatch for slice: seq id %d, start %d, span %d, expected MD5 %s",
										s.sequenceId, s.alignmentStart, s.alignmentSpan,
										String.format("%032x", new BigInteger(1, s.refMD5))));
						if (!params.resilient)
							System.exit(1);
					}
				}

			long time1 = System.nanoTime();
			n.normalize(cramRecords, ref, 0, c.header.substitutionMatrix);
			long time2 = System.nanoTime();
			normTime += time2 - time1;

			Cram2SamRecordFactory c2sFactory = new Cram2SamRecordFactory(cramHeader.getSamFileHeader());

			long c2sTime = 0;
			long sWriteTime = 0;

			boolean enough = false;
			for (CramCompressionRecord r : cramRecords) {
				// enforcing a special way to calculate template size:
				restoreMateInfo(r);

				// check if the record ends before the query start:
				if (location != null && r.sequenceId == location.sequenceId && r.getAlignmentEnd() < location.start)
					continue;

				// we got all the reads for random access:
				if (location != null && location.sequenceId == r.sequenceId && location.end < r.alignmentStart) {
					enough = true;
					break;
				}

				time = System.nanoTime();
				SAMRecord s = c2sFactory.create(r);

				if (params.requiredFlags != 0 && ((params.requiredFlags & s.getFlags()) == 0))
					continue;
				if (params.filteringFlags != 0 && ((params.filteringFlags & s.getFlags()) != 0))
					continue;
				if (params.countOnly) {
					recordCount++;
					baseCount += r.readLength;
					continue;
				}

				if (ref != null)
					Utils.calculateMdAndNmTags(s, ref, params.calculateMdTag, params.calculateNmTag);
				c2sTime += System.nanoTime() - time;
				samTime += System.nanoTime() - time;

				time = System.nanoTime();
				writer.addAlignment(s);
				sWriteTime += System.nanoTime() - time;
				writeTime += System.nanoTime() - time;
				if (params.outputFile == null && System.out.checkError())
					break;

			}

			log.info(String.format("CONTAINER READ: io %dms, parse %dms, norm %dms, convert %dms, BAM write %dms",
					c.readTime / 1000000, c.parseTime / 1000000, (time2 - time1) / 1000000, c2sTime / 1000000,
					sWriteTime / 1000000));

			if (enough || (params.outputFile == null && System.out.checkError()))
				break;
		}

		if (params.countOnly) {
			System.out.printf("READS: %d; BASES: %d\n", recordCount, baseCount);
		}

		writer.close();

		log.warn(String.format("TIMES: io %ds, parse %ds, norm %ds, convert %ds, BAM write %ds", readTime / 1000000000,
				parseTime / 1000000000, normTime / 1000000000, samTime / 1000000000, writeTime / 1000000000));
	}

	private static void restoreMateInfo(CramCompressionRecord r) {
		if (r.next == null) {

			return;
		}
		CramCompressionRecord cur;
		cur = r;
		while (cur.next != null) {
			setNextMate(cur, cur.next);
			cur = cur.next;
		}

		// cur points to the last segment now:
		CramCompressionRecord last = cur;
		setNextMate(last, r);
		// r.setFirstSegment(true);
		// last.setLastSegment(true);

		final int templateLength = Utils.computeInsertSize(r, last);
		r.templateSize = templateLength;
		last.templateSize = -templateLength;
	}

	private static void setNextMate(CramCompressionRecord r, CramCompressionRecord next) {
		r.mateAlignmentStart = next.alignmentStart;
		r.setMateUnmapped(next.isSegmentUnmapped());
		r.setMateNegativeStrand(next.isNegativeStrand());
		r.mateSequenceID = next.sequenceId;
		if (r.mateSequenceID == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
			r.mateAlignmentStart = SAMRecord.NO_ALIGNMENT_START;
	}

	private static htsjdk.samtools.cram.structure.Container skipToContainer(SeekableStream cramFileInputStream, CramHeader header,
			AlignmentSliceQuery location) throws IOException, ReadNotFoundException, URISyntaxException {
		Container c = null;
		String source = cramFileInputStream.getSource();
		if (source == null)
			throw new RuntimeException("Index file not found: null");

		{ // try crai:
			log.info("Reading CRAI...");
			List<CramIndex.Entry> entries;
			try {
				entries = getCraiEntries(source, header, location);
			} catch (URISyntaxException e1) {
				throw new RuntimeException("Failed to read index file for " + source);
			}
			if (entries != null) {

				if (entries.isEmpty())
					throw new ReadNotFoundException();

				log.info("Found index entries: " + entries.size());

				// cramFileInputStream.position(entries.get(0).containerStartOffset);
				// c = CramIO.readContainerHeader(cramFileInputStream);
				// if (c == null)
				// throw new
				// RuntimeException("Index point outside of the file.");

				cramFileInputStream.seek(entries.get(0).containerStartOffset);
				return c;
			}
		}

		{ // try bai:
			log.info("Reading BAI...");
			long[] filePointers;
			try {
				filePointers = getBaiFilePointers(source, header, location);
			} catch (URISyntaxException e1) {
				throw new RuntimeException("Failed to read index for " + cramFileInputStream.getSource());
			}
			if (filePointers == null)
				return null;

			if (filePointers.length == 0)
				throw new ReadNotFoundException();

			for (int i = 0; i < filePointers.length; i += 2) {
				long offset = filePointers[i] >>> 16;
				int sliceIndex = (int) ((filePointers[i] << 48) >>> 48);
				cramFileInputStream.seek(offset);
				// c = CramIO.readContainerHeader(cramFileInputStream);
				// if (c.alignmentStart + c.alignmentSpan > location.start)
				// {
				// cramFileInputStream.position(offset);
				// return c;
				// }
			}
		}

		return c;
	}

	private static long[] getBaiFilePointers(String source, CramHeader header, AlignmentSliceQuery location)
			throws IOException, ReadNotFoundException, URISyntaxException {
		long[] filePointers = null;

		File indexFile = new File(source + ".bai");
		if (!indexFile.exists()) {
			InputStream is = Utils.openInputStreamFromURL(source + ".bai");
			if (is == null)
				return null;

			indexFile = File.createTempFile("", "");
			indexFile.deleteOnExit();
			FileOutputStream fos = new FileOutputStream(indexFile);
			OutputStream os = new BufferedOutputStream(fos);
			IOUtil.copyStream(is, os);
		}

		if (indexFile.exists())
			filePointers = BAMIndexFactory.SHARED_INSTANCE.getBAMIndexPointers(indexFile,
					header.getSamFileHeader().getSequenceDictionary(), location.sequence, location.start, location.end);

		return filePointers;
	}

	private static List<CramIndex.Entry> getCraiEntries(String source, CramHeader header, AlignmentSliceQuery location)
			throws IOException, URISyntaxException {
		InputStream is = Utils.openInputStreamFromURL(source + ".crai");

		if (is != null) {
			GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
			BufferedInputStream bis = new BufferedInputStream(gis);
			List<CramIndex.Entry> full = CramIndex.readIndex(gis);
			Collections.sort(full);

			List<CramIndex.Entry> entries = new LinkedList<CramIndex.Entry>();
			SAMSequenceRecord sequence = header.getSamFileHeader().getSequence(location.sequence);
			if (sequence == null)
				throw new RuntimeException("Sequence not found: " + location.sequence);

			entries.addAll(CramIndex.find(full, sequence.getSequenceIndex(), location.start, location.end
					- location.start));

			bis.close();

			return entries;
		}
		return null;
	}

	private static SAMFileWriter createSAMFileWriter(Params params, CramHeader cramHeader,
			SAMFileWriterFactory samFileWriterFactory) throws IOException {
		/*
		 * building sam writer, sometimes we have to go deeper to get to the
		 * required functionality:
		 */
		SAMFileWriter writer = null;
		if (params.outputFastq) {
			if (params.cramURL == null) {
				writer = new FastqSAMFileWriter(System.out, null, cramHeader.getSamFileHeader());
			} else {
				writer = new FastqSAMFileWriter(Utils.getFileName(params.cramURL), false, cramHeader.getSamFileHeader());

			}
		} else if (params.outputFastqGz) {
			if (params.cramURL == null) {
				GZIPOutputStream gos = new GZIPOutputStream(System.out);
				PrintStream ps = new PrintStream(gos);
				writer = new FastqSAMFileWriter(ps, null, cramHeader.getSamFileHeader());
			} else {
				writer = new FastqSAMFileWriter(Utils.getFileName(params.cramURL), true, cramHeader.getSamFileHeader());

			}
		} else if (params.outputFile == null) {
			OutputStream os = new BufferedOutputStream(System.out);
			if (params.outputBAM) {
				writer = new SAMFileWriterFactory().makeBAMWriter(cramHeader.getSamFileHeader(), true, os);
			} else {
				writer = Utils.createSAMTextWriter(samFileWriterFactory, os, cramHeader.getSamFileHeader(),
						params.printSAMHeader);
			}
		} else {
			writer = samFileWriterFactory.makeSAMOrBAMWriter(cramHeader.getSamFileHeader(), true, params.outputFile);
		}
		return writer;
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-cram-file", "-I" }, description = "The path or FTP URL to the CRAM file to uncompress. Omit if standard input (pipe).")
		String cramURL;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example). ")
		File reference;

		@Parameter(names = { "--output-bam-file", "-O" }, converter = FileConverter.class, description = "The path to the output BAM file.")
		File outputFile;

		@Parameter(names = { "-b", "--output-bam-format" }, description = "Output in BAM format.")
		boolean outputBAM = false;

		@Parameter(names = { "-q", "--output-fastq-format" }, hidden = true, description = "Output in fastq format.")
		boolean outputFastq = false;

		@Parameter(names = { "-z", "--output-fastq-gz-format" }, hidden = true, description = "Output in gzipped fastq format.")
		boolean outputFastqGz = false;

		@Parameter(names = { "--print-sam-header" }, description = "Print SAM header when writing SAM format.")
		boolean printSAMHeader = false;

		@Parameter(names = { "-H" }, description = "Print SAM header and quit.")
		boolean printSAMHeaderOnly = false;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--default-quality-score" }, description = "Use this quality score (decimal representation of ASCII symbol) as a default value when the original quality score was lost due to compression. Minimum is 33.")
		int defaultQS = '?';

		@Parameter(names = { "--calculate-md-tag" }, description = "Calculate MD tag.")
		boolean calculateMdTag = false;

		@Parameter(names = { "--calculate-nm-tag" }, description = "Calculate NM tag.")
		boolean calculateNmTag = false;

		@Parameter(description = "A region to access specified as <sequence name>[:<start inclusive>[-[<stop inclusive>]]")
		List<String> locations;

		@Parameter(names = { "--decrypt" }, description = "Decrypt the file.")
		boolean decrypt = false;

		@Parameter(names = { "--count-only", "-c" }, description = "Count number of records.")
		boolean countOnly = false;

		@Parameter(names = { "--required-flags", "-f" }, description = "Required flags. ")
		int requiredFlags = 0;

		@Parameter(names = { "--filter-flags", "-F" }, description = "Filtering flags. ")
		int filteringFlags = 0;

		@Parameter(names = { "--inject-sq-uri" }, description = "Inject or change the @SQ:UR header fields to point to ENA reference service. ")
		public boolean injectURI = false;

		@Parameter(names = { "--sync-bam-output" }, description = "Write BAM output in the same thread.")
		public boolean syncBamOutput = false;

		@Parameter(names = { "--async-bam-buffer" }, description = "The buffer size (number of records) for the asynchronious BAM output.", hidden = true)
		int asyncBamBuffer = 10000;

		@Parameter(names = { "--ignore-md5-mismatch" }, description = "Issue a warning on sequence MD5 mismatch and continue. This does not garantee the data will be read succesfully. ")
		public boolean ignoreMD5Mismatch = false;

		@Parameter(names = { "--skip-md5-check" }, description = "Skip MD5 checks when reading the header.")
		public boolean skipMD5Checks = false;

		@Parameter(names = { "--ref-seq-download-tries" }, description = "Try to download sequences this many times if their md5 mismatches.", hidden = true)
		int downloadTriesBeforeFailing = 2;

		@Parameter(names = { "--resilient" }, description = "Report reference sequence md5 mismatch and keep going.", hidden = true)
		public boolean resilient = false;

		@Parameter(names = { "--password", "-p" }, description = "Password to decrypt the file.")
		public String password;

		@Parameter(names = { "--max-containers" }, description = "Read only specified number of containers.", hidden = true)
		long maxContainers = Long.MAX_VALUE;
	}

}
