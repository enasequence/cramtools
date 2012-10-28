/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
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
package net.sf.cram.stats;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.cram.CramRecord;
import net.sf.cram.ReadTag;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SubstitutionVariation;

import org.apache.commons.math.stat.HashMapFrequency;

import uk.ac.ebi.ena.sra.compression.huffman.HuffmanCode;
import uk.ac.ebi.ena.sra.compression.huffman.HuffmanTree;

public class CramStats {
	private CramRecord prevRecord;
	private HashMapFrequency inSeqPosFreq = new HashMapFrequency();
	private HashMapFrequency inReadPosFreq = new HashMapFrequency();

	private int[] stopBaseFreqArray = new int[256];
	private int[] stopQSFreqArray = new int[256];

	private HashMapFrequency readLengthFreq = new HashMapFrequency();
	private HashMapFrequency delLengthFreq = new HashMapFrequency();
	private HashMapFrequency readFeatureFreq = new HashMapFrequency();
	private HashMapFrequency distanceToNextFragmentFreq = new HashMapFrequency();
	private HashMapFrequency readGroupIndexFreq = new HashMapFrequency();
	private HashMapFrequency mappingQualityFreq = new HashMapFrequency();

	private Map<String, ByteFrequencies> tagFreqs = new TreeMap<String, ByteFrequencies>();
	private Map<String, IntFrequencies> tagLengths = new TreeMap<String, IntFrequencies>();
	private HashMapFrequency tagKeyAndTypeFrequency = new HashMapFrequency();
	private ByteFrequencies tagCountFrequency = new ByteFrequencies();

	private ByteFrequencies flagStats = new ByteFrequencies();

	private int[] baseFreqArray = new int[256];
	private int[] qsFreqArray = new int[256];

	private DiByteFrequencies qs2Frequency = new DiByteFrequencies();
	private byte prevQS = -1;

	private long nofSubstituions = 0L;
	private long nofInsertions = 0L;
	private long nofInsertedBases = 0L;
	private long nofDeletions = 0L;

	private long baseCount = 0L;
	private long recordCount = 0L;

	private long firstRecordPosition = -1L;

	private HashBag readAnnoKeyBag = new HashBag();

	private final CramHeader header;

	private int[] heapByteFreqArray = new int[256];

	private ByteFrequencies readNameFreqs = new ByteFrequencies();
	private IntFrequencies readNameLengthFreqs = new IntFrequencies();

	public CramStats(CramHeader header) {
		this.header = header;
	}

	public void addRecord(CramRecord record) {
		recordCount++;
		baseCount += record.getReadLength();
		readLengthFreq.addValue(record.getReadLength());

		flagStats.add(record.getFlags());

		if (record.getAnnotations() != null) {
			for (ReadAnnotation a : record.getAnnotations())
				readAnnoKeyBag.add(a.getKey());
		}

		readGroupIndexFreq.addValue(record.getReadGroupID());

		if (record.tags != null) {
			for (ReadTag tag : record.tags) {
				String keyAndType = tag.getKeyAndType();
				tagKeyAndTypeFrequency.addValue(keyAndType);

				ByteFrequencies bf = tagFreqs.get(keyAndType);
				if (bf == null) {
					bf = new ByteFrequencies();
					tagFreqs.put(keyAndType, bf);
				}
				byte[] bytes = tag.getValueAsByteArray();
				bf.add(bytes);

				IntFrequencies bl = tagLengths.get(keyAndType);
				if (bl == null) {
					bl = new IntFrequencies();
					tagLengths.put(keyAndType, bl);
				}
				bl.add(bytes.length);
			}

			tagCountFrequency.add((byte) record.tags.size());
		} else
			tagCountFrequency.add((byte) 0);

		if (!record.isLastFragment())
			distanceToNextFragmentFreq.addValue(record.getRecordsToNextFragment());

		if (record.getAlignmentStart() > 0) {
			if (firstRecordPosition < 0)
				firstRecordPosition = record.getAlignmentStart();

			if (prevRecord == null) {
				inSeqPosFreq.addValue(0);
			} else {
				inSeqPosFreq.addValue(record.getAlignmentStart() - prevRecord.getAlignmentStart());
			}
			prevRecord = record;
		}

		if (!record.isReadMapped()) {
			byte[] bases = record.getReadBases();
			byte[] scores = record.getQualityScores();
			for (int i = 0; i < record.getReadLength(); i++) {
				if (bases != null) 
					baseFreqArray[bases[i]]++;
				if (scores != null && scores.length != 0) 
					qsFreqArray[scores[i]]++;
			}

		}
		mappingQualityFreq.addValue(record.getMappingQuality());

		Collection<ReadFeature> variationsByPosition = record.getReadFeatures();
		if (variationsByPosition != null) {
			int prevInReadPos = 0;
			for (ReadFeature f : variationsByPosition) {
				readFeatureFreq.addValue(f.getOperator());

				inReadPosFreq.addValue(f.getPosition() - prevInReadPos);
				switch (f.getOperator()) {
				case SubstitutionVariation.operator:
					SubstitutionVariation sv = (SubstitutionVariation) f;
					if (sv.getBase() != 0) {
						baseFreqArray[sv.getBase()]++;
					}
					nofSubstituions++;
					break;
				case ReadBase.operator:
					ReadBase rb = (ReadBase) f;
					baseFreqArray[rb.getBase()]++;
					qsFreqArray[rb.getQualityScore()]++;
					break;
				case InsertionVariation.operator:
					InsertionVariation iv = (InsertionVariation) f;
					for (byte base : iv.getSequence())
						stopBaseFreqArray[base]++;
					nofInsertions++;
					nofInsertedBases += iv.getSequence().length;

					// the only use of stop- freqs are insertions, because they
					// are short sequences:
					stopBaseFreqArray['$']++;
					stopQSFreqArray[-1]++;
					break;
				case DeletionVariation.operator:
					DeletionVariation dv = (DeletionVariation) f;
					nofDeletions++;
					delLengthFreq.addValue(dv.getLength());
					break;
				case InsertBase.operator:
					InsertBase ib = (InsertBase) f;
					baseFreqArray[ib.getBase()]++;
					nofInsertions++;
					nofInsertedBases++;
					break;
				case BaseQualityScore.operator:
					BaseQualityScore bqs = (BaseQualityScore) f;
					qsFreqArray[bqs.getQualityScore()]++;
					break;

				default:
					break;
				}
				prevInReadPos = f.getPosition();
			}
			if (!variationsByPosition.isEmpty())
				readFeatureFreq.addValue(ReadFeature.STOP_OPERATOR);
		}

		boolean readNameAdded = false;
		if (record.getReadName() != null) {
			byte[] readNameBytes = record.getReadName().getBytes();
			readNameFreqs.add(readNameBytes);
			readNameLengthFreqs.add(readNameBytes.length);
			readNameAdded = true;
		}

		CramRecord mate = record.next == null ? record.previous : record.next;
		if (mate != null) {
			if (!readNameAdded) {
				byte[] readNameBytes = record.getReadName().getBytes();
				readNameFreqs.add(readNameBytes);
				readNameLengthFreqs.add(readNameBytes.length);
			}
			addString(String.valueOf(mate.getAlignmentStart()));
			addString(String.valueOf(mate.getSequenceName()));
			addString(String.valueOf(record.insertSize));
		}
	}

	private void addString(String s) {
		for (byte b : s.getBytes())
			heapByteFreqArray[b]++;
		heapByteFreqArray[0]++;
	}

	public void adjustBlock(CramRecordBlock block) throws CramCompressionException {
		block.setFirstRecordPosition(firstRecordPosition);
		block.setRecordCount(recordCount);

		CramCompression compression = block.getCompression();
		if (compression == null) {
			block.setCompression(new CramCompression());
			compression = block.getCompression();
		}

		compression.flagStats = flagStats;

		compression.tagKeyAlphabet = new String[0];
		compression.tagKeyFrequency = new int[0];
		int tagCount = tagKeyAndTypeFrequency.getUniqueCount();
		compression.tagKeyAlphabet = new String[tagCount];
		compression.tagKeyFrequency = new int[tagCount];
		Iterator<Comparable<?>> tagIterator = tagKeyAndTypeFrequency.valuesIterator();
		for (int i = 0; i < tagCount; i++) {
			String keyAndType = (String) tagIterator.next();
			compression.tagKeyAlphabet[i] = keyAndType;
			compression.tagKeyFrequency[i] = (int) tagKeyAndTypeFrequency.getCount(keyAndType);

			if (keyAndType.startsWith("OQ") || keyAndType.startsWith("BQ") || keyAndType.startsWith("BC")
					|| keyAndType.startsWith("CQ") || keyAndType.startsWith("Q2"))
				if (tagLengths.get(keyAndType).getValues().length > 8)
					tagFreqs.remove(keyAndType);
			char type = keyAndType.charAt(3);
			if (type == 'B')
				tagFreqs.remove(keyAndType);
		}
		compression.tagByteFrequencyMap = tagFreqs;
		compression.tagByteLengthMap = tagLengths;
		compression.tagCountFrequency = tagCountFrequency;

		ValueFrequencyHolder holder = getValueFrequencies(baseFreqArray);
		compression.setBaseAlphabet(holder.values);
		compression.setBaseFrequencies(holder.frequencies);

		holder = getValueFrequencies(qsFreqArray);
		compression.setScoreAlphabet(holder.values);
		compression.setScoreFrequencies(holder.frequencies);

		compression.score2 = qs2Frequency;

		holder = getValueFrequencies(stopBaseFreqArray);
		compression.setStopBaseAlphabet(holder.values);
		compression.setStopBaseFrequencies(holder.frequencies);

		holder = getValueFrequencies(stopQSFreqArray);
		compression.setStopScoreAlphabet(holder.values);
		compression.setStopScoreFrequencies(holder.frequencies);

		if (readLengthFreq.getUniqueCount() == 1)
			block.setReadLength(((Long) readLengthFreq.valuesIterator().next()).intValue());
		else
			block.setReadLength(0);

		holder = getValueFrequencies(readLengthFreq);
		compression.setReadLengthAlphabet(holder.intValues);
		compression.setReadLengthFrequencies(holder.frequencies);

		holder = getValueFrequencies(mappingQualityFreq);
		compression.setMappingQualityAlphabet(holder.values);
		compression.setMappingQualityFrequencies(holder.frequencies);

		block.getCompression().setInSeqPosEncoding(getEncoding(inSeqPosFreq));
		block.getCompression().setInReadPosEncoding(getEncoding(inReadPosFreq));
		block.getCompression().setDelLengthEncoding(getEncoding(delLengthFreq));
		block.getCompression().setReadLengthEncoding(getEncoding(readLengthFreq));
		block.getCompression().setRecordsToNextFragmentEncoding(getEncoding(distanceToNextFragmentFreq));

		holder = getValueFrequencies(readFeatureFreq);
		compression.setReadFeatureAlphabet(holder.values);
		compression.setReadFeatureFrequencies(holder.frequencies);

		if (header != null && header.getReadAnnotations() != null) {
			Set uniqueSet = readAnnoKeyBag.uniqueSet();
			int[] raIndexes = new int[uniqueSet.size()];
			int[] raFreqs = new int[raIndexes.length];
			int i = 0;
			for (Object o : uniqueSet) {
				ReadAnnotation ra = (ReadAnnotation) o;
				int count = readAnnoKeyBag.getCount(ra);
				if (count == 0)
					continue;

				int index = header.getReadAnnotations().indexOf(ra);
				if (index < 0)
					throw new CramCompressionException("Annotation not found in the dictionary: " + ra.getKey());

				raIndexes[i] = index;
				raFreqs[i] = count;
				i++;
			}
			compression.setReadAnnotationIndexes(raIndexes);
			compression.setReadAnnotationFrequencies(raFreqs);
		} else {
			compression.setReadAnnotationIndexes(new int[0]);
			compression.setReadAnnotationFrequencies(new int[0]);
		}

		holder = getValueFrequencies(readGroupIndexFreq);
		compression.setReadGroupIndexes(holder.intValues);
		compression.setReadGroupFrequencies(holder.frequencies);

		holder = getValueFrequencies(heapByteFreqArray);
		compression.setHeapByteAlphabet(holder.values);
		compression.setHeapByteFrequencies(holder.frequencies);

		compression.readNameFreqs = readNameFreqs;
		compression.readNameLengthFreqs = readNameLengthFreqs;

	}

	private static final Encoding getEncoding(HashMapFrequency f) throws CramCompressionException {
		NumberCodecOptimiser optimiser = new NumberCodecOptimiser();
		Iterator<Comparable<?>> valuesIterator = f.valuesIterator();
		int i = 0;
		long valueCounter = 0;
		long maxInSeqPos = Long.MIN_VALUE;
		while (valuesIterator.hasNext()) {
			Comparable<?> next = valuesIterator.next();
			Long value = (Long) next;
			if (maxInSeqPos < value)
				maxInSeqPos = value;
			i++;
			long count = f.getCount(next);
			valueCounter += count;
			optimiser.addValue(value, count);
		}

		// long huffmanBits = getHuffmanBitLengthEstimate(f);
		// if (optimiser.getMinLength() > huffmanBits) {
		// System.out.printf("Huffman is better: %d vs %d\n",
		// optimiser.getMinLength(), huffmanBits);
		// } else
		// System.out.printf("Huffman is worse: %d vs %d\n",
		// optimiser.getMinLength(), huffmanBits);

		long binaryBitsRequiredPerPosition;
		if (maxInSeqPos < 1)
			binaryBitsRequiredPerPosition = 0;
		else
			binaryBitsRequiredPerPosition = (long) Math.ceil(Math.log(maxInSeqPos + 1) / Math.log(2));
		long totalBinaryBitsRequired = binaryBitsRequiredPerPosition * valueCounter;

		if (optimiser.getMinLength() > totalBinaryBitsRequired) {
			return new Encoding(EncodingAlgorithm.BETA, "0," + binaryBitsRequiredPerPosition);
		} else {
			NumberCodecStub inSeqPosStub = optimiser.getMinLengthStub();
			return new Encoding(inSeqPosStub.getEncoding(), inSeqPosStub.getStringRepresentation());
		}
	}

	public static long getHuffmanBitLengthEstimate(HashMapFrequency f) {
		Iterator<Comparable<?>> valuesIterator = f.valuesIterator();
		int i = 0;
		Long[] values = new Long[f.getUniqueCount()];
		int[] freqs = new int[values.length];
		while (valuesIterator.hasNext()) {
			Comparable<?> next = valuesIterator.next();
			values[i] = ((Long) next).longValue();
			freqs[i] = (int) f.getCount(next);
			i++;
		}
		HuffmanTree<Long> tree = HuffmanCode.buildTree(freqs, values);

		valuesIterator = f.valuesIterator();
		HuffmanCodec<Long> codec = new HuffmanCodec<Long>(tree);
		long totalBits = 0;
		while (valuesIterator.hasNext()) {
			Comparable<?> next = valuesIterator.next();
			long bits = codec.numberOfBits(((Long) next).longValue());
			totalBits += bits * (int) f.getCount(next);
		}

		// length of int array plus length of int array in bits:
		int treeLen = ((4 + values.length * 4) + (4 + freqs.length * 4)) * 8;
		return totalBits + treeLen;
	}

	private static class ValueFrequencyHolder {
		byte[] values;
		int[] intValues;
		int[] frequencies;

		public ValueFrequencyHolder(int size) {
			values = new byte[size];
			intValues = new int[size];
			frequencies = new int[size];
		}
	}

	private static final ValueFrequencyHolder getValueFrequencies(HashMapFrequency f) {
		ValueFrequencyHolder holder = new ValueFrequencyHolder(f.getUniqueCount());
		Iterator<Comparable<?>> valuesIterator = f.valuesIterator();
		int i = 0;
		int[] array = new int[f.getUniqueCount()];
		while (valuesIterator.hasNext()) {
			array[i++] = ((Long) (valuesIterator.next())).intValue();
		}

		Arrays.sort(array);
		for (i = 0; i < array.length; i++) {
			int next = array[i];
			holder.values[i] = (byte) next;
			holder.intValues[i] = next;
			holder.frequencies[i] = (int) f.getCount(next);
		}

		return holder;
	}

	private static final ValueFrequencyHolder getValueFrequencies(int[] array) {
		int nonZeroCount = 0;
		for (int v : array)
			if (v > 0)
				nonZeroCount++;

		ValueFrequencyHolder holder = new ValueFrequencyHolder(nonZeroCount);
		int vIndex = 0;
		for (int i = 0; i < array.length; i++) {
			if (array[i] > 0) {
				holder.values[vIndex] = (byte) i;
				holder.intValues[vIndex] = i;
				holder.frequencies[vIndex] = array[i];
				vIndex++;
			}
		}

		return holder;
	}

	public long getBaseCount() {
		return baseCount;
	}
}
