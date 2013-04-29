package net.sf.cram.ref;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;

public class InMemoryReferenceSequenceFile implements ReferenceSequenceFile {
	private Map<Integer, byte[]> sequences = new HashMap<Integer, byte[]>();
	private SAMSequenceDictionary dictionary = new SAMSequenceDictionary();
	private int currentIndex = 0;

	public void addSequence(String name, byte[] bases) {
		SAMSequenceRecord r = new SAMSequenceRecord(name, bases.length);
		dictionary.addSequence(r);
		int index = getSequenceDictionary().getSequenceIndex(name);
		sequences.put(index, bases);
	}

	@Override
	public ReferenceSequence getSequence(String name) {
		int index = getSequenceDictionary().getSequenceIndex(name);
		return new ReferenceSequence(name, index, sequences.get(index));
	}

	@Override
	public SAMSequenceDictionary getSequenceDictionary() {
		return dictionary;
	}

	@Override
	public ReferenceSequence getSubsequenceAt(String name, long start, long stop) {
		int index = getSequenceDictionary().getSequenceIndex(name);
		byte[] bases = Arrays.copyOfRange(sequences.get(index), (int) start,
				(int) stop + 1);
		return new ReferenceSequence(name, index, bases);
	}

	/**
	 * @param name
	 * @param start
	 *            inclusive
	 * @param stop
	 *            inclusive
	 * @return
	 */
	public ReferenceRegion getRegion(String name, long start, long stop) {
		int index = getSequenceDictionary().getSequenceIndex(name);
		if (!sequences.containsKey(index))
			throw new RuntimeException("Sequence not found: " + name);

		ReferenceRegion region = new ReferenceRegion(sequences.get(index),
				index, name, start, stop);
		return region;
	}

	@Override
	public boolean isIndexed() {
		return true;
	}

	@Override
	public ReferenceSequence nextSequence() {
		if (currentIndex >= dictionary.size())
			return null;

		SAMSequenceRecord sequence = dictionary.getSequence(currentIndex++);
		return getSequence(sequence.getSequenceName());
	}

	@Override
	public void reset() {
		currentIndex = 0;
	}

}
