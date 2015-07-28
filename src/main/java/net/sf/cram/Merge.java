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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.Log;
import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.CramTools.ValidationStringencyConverter;
import net.sf.cram.FixBAMFileHeader.MD5MismatchError;
import net.sf.cram.common.Utils;
import net.sf.cram.index.BAMQueryFilteringIterator;
import net.sf.cram.index.CramIndex;
import net.sf.cram.ref.ReferenceSource;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Merge {

	public static final String COMMAND = "merge";

	public static void usage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Merge.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		Log.setGlobalLogLevel(params.logLevel);

		if (args.length == 0 || params.help) {
			usage(jc);
			System.exit(1);
		}

		if (params.reference == null) {
			System.out.println("A reference fasta file is required.");
			System.exit(1);
		}

		if (params.files == null || params.files.isEmpty()) {
			System.out.println("At least one CRAM or BAM file is required.");
			System.exit(1);
		}

		ReferenceSource referenceSource = null;
		if (params.reference != null) {
			System.setProperty("reference", params.reference.getAbsolutePath());
			referenceSource = new ReferenceSource(params.reference);
		} else {
			String prop = System.getProperty("reference");
			if (prop != null)
				referenceSource = new ReferenceSource(new File(prop));
		}

		AlignmentSliceQuery query = params.region == null ? null : new AlignmentSliceQuery(params.region);

		List<RecordSource> list = readFiles(params.files, params.reference, query, params.validationLevel);

		StringBuffer mergeComment = new StringBuffer("Merged from:");
		for (RecordSource source : list) {
			mergeComment.append(" ").append(source.path);
		}

		resolveCollisions(list);
		SAMFileHeader header = mergeHeaders(list);
		header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
		FixBAMFileHeader fix = new FixBAMFileHeader(referenceSource);
		fix.setConfirmMD5(true);
		fix.setInjectURI(true);
		fix.setIgnoreMD5Mismatch(false);
		try {
			fix.fixSequences(header.getSequenceDictionary().getSequences());
		} catch (MD5MismatchError e) {
			e.printStackTrace();
			System.exit(1);
		}
		fix.addCramtoolsPG(header);
		header.addComment(mergeComment.toString());

		SAMFileWriter writer = null;
		if (params.outFile != null)
			if (!params.samFormat)
				writer = new SAMFileWriterFactory().makeBAMWriter(header, true, params.outFile);
			else
				writer = new SAMFileWriterFactory().makeSAMWriter(header, true, params.outFile);
		else if (!params.samFormat) {
			// hack to write BAM format to stdout:
			File file = File.createTempFile("bam", null);
			file.deleteOnExit();
			SAMFileWriter bamWriter = new SAMFileWriterFactory().makeBAMWriter(header, true, System.out);
			writer = bamWriter;
		}

		else {
			writer = Utils.createSAMTextWriter(null, System.out, header, params.printSAMHeader);
		}

		MergedIterator mergedIterator = new MergedIterator(list, header);
		while (mergedIterator.hasNext()) {
			SAMRecord record = mergedIterator.next();
			writer.addAlignment(record);
		}

		mergedIterator.close();
		for (RecordSource source : list)
			source.close();

			writer.close();
	}

	private static List<RecordSource> readFiles(List<File> files, File refFile,
			AlignmentSliceQuery query, ValidationStringency ValidationStringency) throws IOException {
		List<RecordSource> sources = new ArrayList<Merge.RecordSource>(files.size());

		SAMFileReader.setDefaultValidationStringency(ValidationStringency);
		for (File file : files) {
			IOUtil.assertFileIsReadable(file);

			RecordSource source = new RecordSource();
			sources.add(source);
			source.id = file.getName();
			source.path = file.getAbsolutePath();

			File index = new File(file.getAbsolutePath() + ".bai");
			if (index.exists()) {
				SAMFileReader reader = new SAMFileReader(file, index);
				source.reader = reader;
				if (query == null)
					source.it = reader.iterator();
				else
					source.it = reader.query(query.sequence, query.start, query.end, true);
			} else {
				index = new File(file.getAbsolutePath() + ".crai");
				if (index.exists()) {
					SAMFileReader reader = new SAMFileReader(file);
					source.reader = reader;
					if (query == null)
						source.it = reader.iterator();
					else {
						SeekableFileStream is = new SeekableFileStream(file);

						FileInputStream fis = new FileInputStream(index);
						GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(fis));
						BufferedInputStream bis = new BufferedInputStream(gis);
						List<CramIndex.Entry> full = CramIndex.readIndex(gis);

						List<CramIndex.Entry> entries = new LinkedList<CramIndex.Entry>();
						SAMSequenceRecord sequence = reader.getFileHeader().getSequence(query.sequence);
						if (sequence == null)
							throw new RuntimeException("Sequence not found: " + query.sequence);

						entries.addAll(CramIndex.find(full, sequence.getSequenceIndex(), query.start, query.end
								- query.start));

						bis.close();

						SamInputResource sir = SamInputResource.of(is);
						final SamReader samReader = SamReaderFactory.make().referenceSequence(refFile).open(sir);
						is.seek(entries.get(0).containerStartOffset);
						BAMQueryFilteringIterator bit = new BAMQueryFilteringIterator(samReader.iterator(), query.sequence, query.start,
								query.end, BAMQueryFilteringIterator.QueryType.CONTAINED, reader.getFileHeader());
						source.it = bit;
					}
				} else {
					SAMFileReader reader = new SAMFileReader(file);
					source.reader = reader;
					source.it = reader.iterator();
				}
			}
		}

		return sources;
	}

	private static List<String> resolveCollisions(List<RecordSource> list) {

		ArrayList<String> result = new ArrayList<String>(list.size());

		// count list entries:
		Map<String, Integer> idCountMap = new TreeMap<String, Integer>();
		for (RecordSource source : list) {
			if (idCountMap.containsKey(source.id)) {
				idCountMap.put(source.id, idCountMap.get(source.id).intValue() + 1);
			} else
				idCountMap.put(source.id, 1);
		}

		// update entries with their number of occurrence except for singletons:
		for (int i = list.size() - 1; i >= 0; i--) {
			RecordSource source = list.get(i);
			int count = idCountMap.get(source.id);
			if (count > 1) {
				list.get(i).id = source.id + String.valueOf(count);
				idCountMap.put(source.id, --count);
			}
		}

		return result;
	}

	private static class RecordSource {
		private String path;
		private String id;
		private CloseableIterator<SAMRecord> it;
		private SAMFileReader reader;

		public RecordSource() {
		}

		public RecordSource(String id, SAMRecordIterator it) {
			this.id = id;
			this.it = it;
		}

		public void close() {
			if (it != null)
				it.close();
			if (reader != null)
				reader.close();
		}

	}

	private static SAMFileHeader mergeHeaders(List<RecordSource> sources) {
		SAMFileHeader header = new SAMFileHeader();
		for (RecordSource source : sources) {
			SAMFileHeader h = source.reader.getFileHeader();

			for (SAMSequenceRecord seq : h.getSequenceDictionary().getSequences()) {
				if (header.getSequenceDictionary().getSequence(seq.getSequenceName()) == null)
					header.addSequence(seq);
			}

			for (SAMProgramRecord pro : h.getProgramRecords()) {
				if (header.getProgramRecord(pro.getProgramGroupId()) == null)
					header.addProgramRecord(pro);
			}

			for (String comment : h.getComments())
				header.addComment(comment);

			for (SAMReadGroupRecord rg : h.getReadGroups()) {
				if (header.getReadGroup(rg.getReadGroupId()) == null)
					header.addReadGroup(rg);
			}
		}

		return header;
	}

	private static class MergedIterator implements SAMRecordIterator {
		private static String delim = ".";
		private RecordSource[] sources;
		private SAMRecord[] records;
		private SAMRecord next;
		private SAMFileHeader header;

		public MergedIterator(List<RecordSource> list, SAMFileHeader header) {
			this.header = header;
			sources = list.toArray(new RecordSource[list.size()]);
			records = new SAMRecord[list.size()];

			for (int i = 0; i < records.length; i++) {
				if (sources[i].it.hasNext())
					records[i] = sources[i].it.next();
			}

			advance();
		}

		@Override
		public void close() {
			if (sources != null)
				for (RecordSource source : sources)
					if (source != null)
						source.close();

			records = null;
			next = null;
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		private void advance() {
			int candidateIndex = getIndexOfMinAlignment();
			if (candidateIndex < 0) {
				next = null;
			} else {
				next = records[candidateIndex];
				SAMSequenceRecord sequence = header.getSequence(next.getReferenceName());

				next.setHeader(header);

				next.setReferenceIndex(sequence.getSequenceIndex());

				next.setReadName(sources[candidateIndex].id + delim + next.getReadName());

				if (next.getMateReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					next.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
				} else {
					SAMSequenceRecord mateSequence = header.getSequence(next.getMateReferenceName());
					next.setMateReferenceIndex(mateSequence.getSequenceIndex());
				}

				if (sources[candidateIndex].it.hasNext())
					records[candidateIndex] = sources[candidateIndex].it.next();
				else
					records[candidateIndex] = null;
			}
		}

		@Override
		public SAMRecord next() {
			if (next == null)
				return null;

			SAMRecord result = next;
			advance();

			return result;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unsupported operation.");
		}

		@Override
		public SAMRecordIterator assertSorted(SAMFileHeader.SortOrder sortOrder) {
			// TODO Auto-generated method stub
			return null;
		}

		private int getIndexOfMinAlignment() {
			if (records == null || records.length == 0)
				return -1;

			int min = Integer.MAX_VALUE;
			int index = -1;
			for (int i = 0; i < records.length; i++) {
				if (records[i] == null)
					continue;

				int start = records[i].getAlignmentStart();
				if (start > 0 && start < min) {
					min = start;
					index = i;
				}
			}
			return index;
		}
	}

	@Parameters(commandDescription = "Tool to merge CRAM or BAM files. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "-v", "--validation-level" }, description = "Change validation stringency level: STRICT, LENIENT, SILENT.", converter = ValidationStringencyConverter.class)
		ValidationStringency validationLevel = ValidationStringency.DEFAULT_STRINGENCY;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "--output-file" }, converter = FileConverter.class, description = "Path to the output BAM file. Omit for stdout.")
		File outFile;

		@Parameter(names = { "--sam-format" }, description = "Output in SAM rather than BAM format.")
		boolean samFormat = false;

		@Parameter(names = { "--sam-header" }, description = "Print SAM file header when output format is text SAM.")
		boolean printSAMHeader = false;

		@Parameter(names = { "--region", "-r" }, description = "Alignment slice specification, for example: chr1:65000-100000.")
		String region;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(converter = FileConverter.class, description = "The paths to the CRAM or BAM files to uncompress. ")
		List<File> files;

	}

}
