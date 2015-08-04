package net.sf.cram.fasta;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Log;
import net.sf.cram.AlignmentSliceQuery;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class BGZF_ReferenceSequenceFile implements ReferenceSequenceFile {
	private static Log log = Log.getInstance(BGZF_ReferenceSequenceFile.class);
	private LinkedHashMap<String, FAIDX_FastaIndexEntry> index = new LinkedHashMap<String, FAIDX_FastaIndexEntry>();
	private Iterator<String> iterator;
	private BlockCompressedInputStream is;
	private SAMSequenceDictionary dictionary;

	public BGZF_ReferenceSequenceFile(File file) throws FileNotFoundException {
		if (!file.canRead())
			throw new RuntimeException("Cannot find or read fasta file: " + file.getAbsolutePath());

		File indexFile = new File(file.getAbsolutePath() + ".fai");
		if (!indexFile.canRead())
			throw new RuntimeException("Cannot find or read fasta index file: " + indexFile.getAbsolutePath());

		Scanner scanner = new Scanner(indexFile);
		int seqID = 0;
		dictionary = new SAMSequenceDictionary();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			FAIDX_FastaIndexEntry entry = FAIDX_FastaIndexEntry.fromString(seqID++, line);
			index.put(entry.getName(), entry);
			dictionary.addSequence(new SAMSequenceRecord(entry.getName(), entry.getLen()));
		}
		scanner.close();

		if (index.isEmpty())
			log.warn("No entries in the index: " + indexFile.getAbsolutePath());

		is = new BlockCompressedInputStream(new SeekableFileStream(file));
	}

	public List<String> lookUpByRegex(String regex) {
		List<String> list = new ArrayList<String>();
		for (String name : index.keySet())
			if (name.matches(regex))
				list.add(name);

		return list;
	}

	@Override
	public SAMSequenceDictionary getSequenceDictionary() {
		return dictionary;
	}

	@Override
	public ReferenceSequence nextSequence() {
		if (iterator == null)
			iterator = index.keySet().iterator();

		if (!iterator.hasNext())
			return null;

		String name = iterator.next();
		try {
			return findSequence(name, 0, 0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void reset() {
		iterator = null;
	}

	@Override
	public boolean isIndexed() {
		return true;
	}

	@Override
	public ReferenceSequence getSequence(String contig) {
		try {
			return findSequence(contig, 0, 0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ReferenceSequence getSubsequenceAt(String contig, long start, long stop) {
		try {
			return findSequence(contig, start, stop);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {

	}

	private ReferenceSequence findSequence(String name, long start, long stop) throws IOException {
		if (!index.containsKey(name))
			return null;

		FAIDX_FastaIndexEntry entry = index.get(name);
		if (start < 1)
			start = 1;

		if (stop < 1)
			stop = start + entry.getLen() - 1;

		if (stop < start)
			throw new RuntimeException("Invalid sequence boundaries.");

		int len = (int) (stop - start) + 1;

		len = Math.min(entry.getLen(), len);

		is.seek(entry.getStartPointer());

		int lineBreakLen = entry.getBytesPerLine() - entry.getBasesPerLine();
		{ // calculate how many bytes to skip from the beginning of the sequence
			// in the file:

			long skip = start + (start / entry.getBasesPerLine()) * lineBreakLen;
			// System.out.println("skip=" + skip);
			is.skip(skip - 1);
		}

		byte[] data = new byte[len];
		int bufPos = 0;

		byte b;
		while ((b = (byte) is.read()) != -1 && bufPos < len && b != '\r' && b != '\n')
			data[bufPos++] = b;

		// skip the rest of "new line" bytes:
		for (int i = 1; i < entry.getBytesPerLine() - entry.getBasesPerLine(); i++)
			is.read();

		// read complete lines:
		int completeLinesToRead = (len - bufPos) / entry.getBasesPerLine();
		for (int line = 0; line < completeLinesToRead; line++) {
			InputStreamUtils.readFully(is, data, bufPos, entry.getBasesPerLine());
			bufPos += entry.getBasesPerLine();
			is.skip(lineBreakLen);
		}

		// read the rest of the last incomplete line:
		while ((b = (byte) is.read()) != -1 && bufPos < len && b != '\r' && b != '\n')
			data[bufPos++] = b;

		return new ReferenceSequence(entry.getName(), entry.getIndex(), data);
	}

	public static void main(String[] args) throws FileNotFoundException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.setProgramName("bquery");
		try {
			jc.parse(args);
		} catch (Exception e) {
			jc.usage();
			return;
		}

		if (params.file == null) {
			jc.usage();
			return;
		}

		BGZF_ReferenceSequenceFile rsf = new BGZF_ReferenceSequenceFile(params.file);

		for (String stringQuery : params.queries) {
			AlignmentSliceQuery q = new AlignmentSliceQuery(stringQuery);
			if (!params.strictSeqNameMatch) {
				List<String> list = lookup(q.sequence, rsf);
				if (list.isEmpty()) {
					log.warn("Sequence not found: " + q.sequence);
					continue;
				}

				if (params.listMatches) {
					for (String seq : list)
						System.out.println(seq);
					continue;
				}

				if (list.size() > 1)
					q.sequence = chooseOne(q.sequence, list);

			}

			ReferenceSequence seq = rsf.getSubsequenceAt(q.sequence, q.start, q.end);
			if (seq == null)
				System.err.println("Nothing found for: " + stringQuery);
			else
				System.out.println(new String(seq.getBases()));
		}
	}

	private static List<String> lookup(String name, BGZF_ReferenceSequenceFile rsf) {
		return rsf.lookUpByRegex("\\b" + name + ".*\\b");
	}

	private static String chooseOne(String query, List<String> list) {
		// grab the one that starts with the query or the first from
		// the list:
		String best = null;
		boolean foundCandidateWhichStartsWithQuery = false;
		for (String candidate : list) {
			if (candidate.startsWith(query)) {
				best = candidate;
				foundCandidateWhichStartsWithQuery = true;
				break;
			}
		}

		if (!foundCandidateWhichStartsWithQuery)
			best = list.get(0);

		log.warn(String.format("Assuming '%s' means '%s'", query, best));

		return best;
	}

	@Parameters(commandDescription = "BGZF fasta utility")
	static class Params {
		@Parameter(names = { "-I" }, description = "Block compressed fasta file.", converter = FileConverter.class)
		File file;

		@Parameter(description = "Regions to fetch, each region should follow the rule: <seq name>[:<start>][-<stop>]]")
		List<String> queries;

		@Parameter(names = { "--strict" }, description = "Match sequence names exactly as in the queries. ")
		boolean strictSeqNameMatch = false;

		@Parameter(names = { "--list-matches" }, description = "Print out a list of matching names.")
		boolean listMatches = false;
	}
}
