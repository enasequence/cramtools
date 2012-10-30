package net.sf.cram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.cram.CompressionHeader;
import net.sf.cram.CramRecord;
import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;
import net.sf.cram.Utils;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.ByteArrayLenEncoding;
import net.sf.cram.encoding.Encoding;
import net.sf.cram.encoding.ExternalByteArrayEncoding;
import net.sf.cram.encoding.ExternalByteEncoding;
import net.sf.cram.encoding.ExternalIntegerEncoding;
import net.sf.cram.encoding.GolombEncoding;
import net.sf.cram.encoding.HuffmanByteEncoding;
import net.sf.cram.encoding.HuffmanIntegerEncoding;
import net.sf.cram.encoding.NullEncoding;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertionVariation;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import uk.ac.ebi.ena.sra.compression.huffman.HuffmanCode;
import uk.ac.ebi.ena.sra.compression.huffman.HuffmanTree;

public class CompressionHeaderFactory {

	public CompressionHeader build(List<CramRecord> records) {
		CompressionHeader h = new CompressionHeader();
		h.externalIds = new ArrayList<>();
		int exCounter = 0;

		int baseID = exCounter++;
		h.externalIds.add(baseID);

		int qualityScoreID = exCounter++;
		h.externalIds.add(qualityScoreID);

		int readNameID = exCounter++;
		h.externalIds.add(readNameID);

		int mateInfoID = exCounter++;
		h.externalIds.add(mateInfoID);

		h.eMap = new TreeMap<>();
		for (EncodingKey key : EncodingKey.values())
			h.eMap.put(key, NullEncoding.toParam());

		h.tMap = new TreeMap<>();

		{ // bit flags encoding:
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getFlags());
			calculator.calculate();
			h.eMap.put(EncodingKey.BF_BitFlags, HuffmanIntegerEncoding.toParam(
					calculator.values(), calculator.bitLens()));
		}

		{ // read length encoding:
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadLength());
			calculator.calculate();

			h.eMap.put(EncodingKey.RL_ReadLength, HuffmanIntegerEncoding
					.toParam(calculator.values(), calculator.bitLens()));
		}

		{ // alignment offset:
			NumberEncodingCalculator calc = new NumberEncodingCalculator();
			for (CramRecord r : records) {
				calc.addValue(r.alignmentStartOffsetFromPreviousRecord);
			}

			Encoding<Long> bestEncoding = calc.getBestEncoding();
			h.eMap.put(
					EncodingKey.AP_AlignmentPositionOffset,
					new EncodingParams(bestEncoding.id(), bestEncoding
							.toByteArray()));
		}

		{ // read group
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadGroupID());
			calculator.calculate();

			h.eMap.put(EncodingKey.RG_ReadGroup, HuffmanIntegerEncoding
					.toParam(calculator.values(), calculator.bitLens()));
		}

		{ // read name encoding:
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadName().length());
			calculator.calculate();

			h.eMap.put(EncodingKey.RN_ReadName, ByteArrayLenEncoding.toParam(
					HuffmanIntegerEncoding.toParam(calculator.values(),
							calculator.bitLens()), ExternalByteArrayEncoding
							.toParam(readNameID)));
		}

		{ // records to next fragment
			NumberEncodingCalculator calc = new NumberEncodingCalculator();
			for (CramRecord r : records)
				calc.addValue(r.getRecordsToNextFragment());

			Encoding<Long> bestEncoding = calc.getBestEncoding();
			h.eMap.put(
					EncodingKey.NF_RecordsToNextFragment,
					new EncodingParams(bestEncoding.id(), bestEncoding
							.toByteArray()));
		}

		// tag count

		// tag name and type

		// tag values

		{ // number of read features
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadFeatures().size());
			calculator.calculate();

			h.eMap.put(EncodingKey.FN_NumberOfReadFeatures,
					HuffmanIntegerEncoding.toParam(calculator.values,
							calculator.bitLens));
		}

		{ // feature position
			NumberEncodingCalculator calc = new NumberEncodingCalculator();
			for (CramRecord r : records) {
				int prevPos = 0;
				for (ReadFeature rf : r.getReadFeatures()) {
					calc.addValue(rf.getPosition() - prevPos);
					prevPos = rf.getPosition();
				}
			}

			Encoding<Long> bestEncoding = calc.getBestEncoding();
			h.eMap.put(EncodingKey.FP_FeaturePosition, new EncodingParams(
					bestEncoding.id(), bestEncoding.toByteArray()));
		}

		{ // feature code
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				for (ReadFeature rf : r.getReadFeatures())
					calculator.add(rf.getOperator());
			calculator.calculate();

			h.eMap.put(EncodingKey.FC_FeatureCode, HuffmanByteEncoding
					.toParam(calculator.valuesAsBytes(), calculator.bitLens));
		}

		{ // bases:
			h.eMap.put(EncodingKey.BA_Base,
					ExternalByteEncoding.toParam(baseID));
		}

		{ // quality scores:
			h.eMap.put(EncodingKey.QS_QualityScore,
					ExternalByteEncoding.toParam(qualityScoreID));
		}

		{ // base substitution code
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				for (ReadFeature rf : r.getReadFeatures())
					if (rf.getOperator() == SubstitutionVariation.operator)
						calculator.add(((SubstitutionVariation) rf)
								.getBaseChange().getChange());
			calculator.calculate();

			h.eMap.put(EncodingKey.BS_BaseSubstitutionCode,
					HuffmanIntegerEncoding.toParam(calculator.values,
							calculator.bitLens));
		}

		{ // insertion bases
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadName().length());
			for (CramRecord r : records)
				for (ReadFeature rf : r.getReadFeatures())
					if (rf.getOperator() == InsertionVariation.operator)
						calculator
								.add(((InsertionVariation) rf).getSequence().length);
			calculator.calculate();

			h.eMap.put(EncodingKey.IN_Insertion, ByteArrayLenEncoding.toParam(
					HuffmanIntegerEncoding.toParam(calculator.values(),
							calculator.bitLens()), ExternalByteArrayEncoding
							.toParam(baseID)));
		}

		{ // deletion length
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				for (ReadFeature rf : r.getReadFeatures())
					if (rf.getOperator() == DeletionVariation.operator)
						calculator.add(((DeletionVariation) rf).getLength());
			calculator.calculate();

			h.eMap.put(EncodingKey.DL_DeletionLength, HuffmanIntegerEncoding
					.toParam(calculator.values, calculator.bitLens));
		}

		{ // mapping quality score
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getMappingQuality());
			calculator.calculate();

			h.eMap.put(EncodingKey.MQ_MappingQualityScore, HuffmanByteEncoding
					.toParam(calculator.valuesAsBytes(), calculator.bitLens));
		}

		{ // mate bit flags
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getMateFlags());
			calculator.calculate();

			h.eMap.put(EncodingKey.MF_MateBitFlags, HuffmanIntegerEncoding
					.toParam(calculator.values, calculator.bitLens));
		}

		{ // next fragment ref id
			h.eMap.put(EncodingKey.NS_NextFragmentReferenceSequenceID,
					ExternalIntegerEncoding.toParam(mateInfoID));
		}

		{ // next fragment alignment start
			h.eMap.put(EncodingKey.NP_NextFragmentAlignmentStart,
					ExternalIntegerEncoding.toParam(mateInfoID));
		}

		{ // template size
			h.eMap.put(EncodingKey.TS_InsetSize,
					ExternalIntegerEncoding.toParam(mateInfoID));
		}

		return h;
	}

	private static class MutableInt {
		public int value = 0;
	}

	private static class HuffmanParamsCalculator {
		private HashMap<Integer, MutableInt> countMap = new HashMap<>();
		private int[] values = new int[] {};
		private int[] bitLens = new int[] {};

		public void add(int huffmanValue) {
			MutableInt counter = countMap.get(huffmanValue);
			if (counter == null) {
				counter = new MutableInt();
				countMap.put(huffmanValue, counter);
			}
			counter.value++;
		}

		public int[] bitLens() {
			return bitLens;
		}

		public int[] values() {
			return values;
		}

		public Integer[] valuesAsAutoIntegers() {
			Integer[] ivalues = new Integer[values.length];
			for (int i = 0; i < ivalues.length; i++)
				ivalues[i] = values[i];

			return ivalues;
		}

		public byte[] valuesAsBytes() {
			byte[] bvalues = new byte[values.length];
			for (int i = 0; i < bvalues.length; i++)
				bvalues[i] = (byte) (0xFF & values[i]);

			return bvalues;
		}

		public Byte[] valuesAsAutoBytes() {
			Byte[] bvalues = new Byte[values.length];
			for (int i = 0; i < bvalues.length; i++)
				bvalues[i] = (byte) (0xFF & values[i]);

			return bvalues;
		}

		public void calculate() {
			HuffmanTree<Integer> tree = null;
			{
				int size = countMap.size();
				int[] freqs = new int[size];
				int[] values = new int[size];

				int i = 0;
				for (Integer v : countMap.keySet()) {
					values[i] = v;
					freqs[i] = countMap.get(v).value;
					i++;
				}
				tree = HuffmanCode.buildTree(freqs, Utils.autobox(values));
			}

			List<Integer> valueList = new ArrayList<>();
			List<Integer> lens = new ArrayList<>();
			HuffmanCode.getValuesAndBitLengths(valueList, lens, tree);

			values = new int[valueList.size()];
			bitLens = new int[valueList.size()];

			for (int i = 0; i < bitLens.length; i++) {
				bitLens[i] = lens.get(i);
				values[i] = valueList.get(i);
			}
		}
	}

	private static class EncodingLengthCalculator {
		private BitCodec<Long> codec;
		private Encoding<Long> encoding;
		private long len;

		public EncodingLengthCalculator(Encoding<Long> encoding) {
			this.encoding = encoding;
			codec = encoding.buildCodec(null, null);
		}

		public void add(long value) {
			len += codec.numberOfBits(value);
		}

		public long len() {
			return len;
		}
	}

	private static class NumberEncodingCalculator {
		private List<EncodingLengthCalculator> calcs = new ArrayList<>();

		public NumberEncodingCalculator() {
			for (int i = 2; i < 20; i++)
				calcs.add(new EncodingLengthCalculator(new GolombEncoding(i)));
		}

		public void addValue(long value) {
			for (EncodingLengthCalculator c : calcs)
				c.add(value);
		}

		public Encoding<Long> getBestEncoding() {
			EncodingLengthCalculator bestC = calcs.get(0);

			for (EncodingLengthCalculator c : calcs) {
				if (c.len() < bestC.len())
					bestC = c;
			}

			return bestC.encoding;
		}
	}
}
