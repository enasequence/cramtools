package net.sf.cram.stats;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.CramRecord;
import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;
import net.sf.cram.ReadTag;
import net.sf.cram.Utils;
import net.sf.cram.encoding.BetaIntegerEncoding;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.ByteArrayLenEncoding;
import net.sf.cram.encoding.ByteArrayStopEncoding;
import net.sf.cram.encoding.Encoding;
import net.sf.cram.encoding.ExternalByteArrayEncoding;
import net.sf.cram.encoding.ExternalByteEncoding;
import net.sf.cram.encoding.ExternalIntegerEncoding;
import net.sf.cram.encoding.GammaIntegerEncoding;
import net.sf.cram.encoding.GolombIntegerEncoding;
import net.sf.cram.encoding.GolombRiceIntegerEncoding;
import net.sf.cram.encoding.HuffmanByteEncoding;
import net.sf.cram.encoding.HuffmanIntegerEncoding;
import net.sf.cram.encoding.NullEncoding;
import net.sf.cram.encoding.SubexpIntegerEncoding;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.cram.huffman.HuffmanCode;
import net.sf.cram.huffman.HuffmanTree;
import net.sf.cram.structure.CompressionHeader;
import net.sf.picard.util.Log;

public class CompressionHeaderFactory {
	private static final Charset charset = Charset.forName("US-ASCII");
	private static Log log = Log.getInstance(CompressionHeaderFactory.class);
	private static final int oqz = ReadTag.nameType3BytesToInt("OQ", 'Z');
	private static final int bqz = ReadTag.nameType3BytesToInt("OQ", 'Z');

	public CompressionHeader build(List<CramRecord> records) {
		CompressionHeader h = new CompressionHeader();
		h.externalIds = new ArrayList<Integer>();
		int exCounter = 0;

		int baseID = exCounter++;
		h.externalIds.add(baseID);

		int qualityScoreID = exCounter++;
		h.externalIds.add(qualityScoreID);

		int readNameID = exCounter++;
		h.externalIds.add(readNameID);

		int mateInfoID = exCounter++;
		h.externalIds.add(mateInfoID);

		int tagValueExtID = exCounter++;
		h.externalIds.add(tagValueExtID);

		log.debug("Assigned external id to bases: " + baseID);
		log.debug("Assigned external id to quality scores: " + qualityScoreID);
		log.debug("Assigned external id to read names: " + readNameID);
		log.debug("Assigned external id to mate info: " + mateInfoID);
		log.debug("Assigned external id to tag values: " + tagValueExtID);

		h.eMap = new TreeMap<EncodingKey, EncodingParams>();
		for (EncodingKey key : EncodingKey.values())
			h.eMap.put(key, NullEncoding.toParam());

		h.tMap = new TreeMap<Integer, EncodingParams>();

		{ // bit flags encoding:
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getFlags());
			calculator.calculate();
			h.eMap.put(EncodingKey.BF_BitFlags, HuffmanIntegerEncoding.toParam(
					calculator.values(), calculator.bitLens()));
		}

		{ // compression bit flags encoding:
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getCompressionFlags());
			calculator.calculate();
			h.eMap.put(EncodingKey.CF_CompressionBitFlags, HuffmanByteEncoding
					.toParam(calculator.valuesAsBytes(), calculator.bitLens()));
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
			IntegerEncodingCalculator calc = new IntegerEncodingCalculator(
					"alignment offset");
			for (CramRecord r : records) {
				calc.addValue(r.alignmentStartOffsetFromPreviousRecord);
			}

			Encoding<Integer> bestEncoding = calc.getBestEncoding();
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
			// h.eMap.put(EncodingKey.RN_ReadName,
			// ByteArrayStopEncoding.toParam((byte) 0, readNameID));
		}

		{ // records to next fragment
			IntegerEncodingCalculator calc = new IntegerEncodingCalculator(
					"records to next fragment");
			for (CramRecord r : records)
				calc.addValue(r.getRecordsToNextFragment());

			Encoding<Integer> bestEncoding = calc.getBestEncoding();
			h.eMap.put(
					EncodingKey.NF_RecordsToNextFragment,
					new EncodingParams(bestEncoding.id(), bestEncoding
							.toByteArray()));
		}

		{ // tag count
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.tags == null ? 0 : r.tags.size());
			calculator.calculate();

			h.eMap.put(EncodingKey.TC_TagCount, HuffmanIntegerEncoding.toParam(
					calculator.values(), calculator.bitLens()));
		}

		{ // tag name and type
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records) {
				if (r.tags == null)
					continue;
				for (ReadTag tag : r.tags)
					calculator.add(tag.keyType3BytesAsInt);
			}
			calculator.calculate();

			h.eMap.put(EncodingKey.TN_TagNameAndType, HuffmanIntegerEncoding
					.toParam(calculator.values(), calculator.bitLens()));
			// h.eMap.put(EncodingKey.TN_TagNameAndType,
			// ExternalByteArrayEncoding.toParam(tagValueExtID));
		}

		// { // EXPERIMENT: tag count, name and type
		// Map<Integer, MutableInt> map = new HashMap<Integer,
		// CompressionHeaderFactory.MutableInt>() ;
		// for (CramRecord r : records) {
		// if (r.tags == null)
		// continue;
		// for (ReadTag tag : r.tags) {
		// MutableInt mutableInt = map.get(tag.keyType3BytesAsInt) ;
		// if (mutableInt == null) {
		// mutableInt = new MutableInt() ;
		// map.put(tag.keyType3BytesAsInt, mutableInt) ;
		// }
		//
		// mutableInt.value++ ;
		// }
		// }
		//
		// System.out.println("Tag codes: ");
		// for (int value:map.keySet()) {
		// System.out.println(value + ": " + map.get(value).value);
		// }
		//
		//
		//
		// HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
		// for (CramRecord r : records) {
		// if (r.tags == null)
		// continue;
		// for (ReadTag tag : r.tags) {
		//
		// calculator.add(tag.keyType3BytesAsInt);
		//
		// }
		// }
		// calculator.calculate();
		//
		// }

		{ // tag values
			Map<Integer, HuffmanParamsCalculator> cc = new TreeMap<Integer, HuffmanParamsCalculator>();

			for (CramRecord r : records) {
				if (r.tags == null)
					continue;

				for (ReadTag tag : r.tags) {
					switch (tag.keyType3BytesAsInt) {
					// case ReadTag.OQZ:
					// case ReadTag.BQZ:
					// EncodingParams params = h.tMap
					// .get(tag.keyType3BytesAsInt);
					// if (params == null) {
					// h.tMap.put(tag.keyType3BytesAsInt,
					// ByteArrayStopEncoding.toParam((byte) 1,
					// tagValueExtID));
					// }
					// break;

					default:
						HuffmanParamsCalculator c = cc
								.get(tag.keyType3BytesAsInt);
						if (c == null) {
							c = new HuffmanParamsCalculator();
							cc.put(tag.keyType3BytesAsInt, c);
						}
						c.add(tag.getValueAsByteArray().length);
						break;
					}
				}
			}

			if (!cc.isEmpty())
				for (Integer key : cc.keySet()) {
					HuffmanParamsCalculator c = cc.get(key);
					c.calculate();

					h.tMap.put(key, ByteArrayLenEncoding.toParam(
							HuffmanIntegerEncoding.toParam(c.values(),
									c.bitLens()),
							ExternalByteArrayEncoding.toParam(tagValueExtID)));
				}

			for (Integer key : h.tMap.keySet()) {
				log.debug(String.format("TAG ENCODING: %d, %s", key,
						h.tMap.get(key)));
			}

			// for (CramRecord r : records) {
			// if (r.tags == null || r.tags.isEmpty())
			// continue;
			// for (ReadTag tag : r.tags) {
			// EncodingParams params = h.tMap.get(tag.keyType3BytesAsInt);
			// if (params == null) {
			// h.tMap.put(tag.keyType3BytesAsInt,
			// ByteArrayStopEncoding.toParam((byte) 0,
			// tagValueExtID));
			// }
			// }
			// }
		}

		{ // number of read features
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				calculator.add(r.getReadFeatures() == null ? 0 : r
						.getReadFeatures().size());
			calculator.calculate();

			h.eMap.put(EncodingKey.FN_NumberOfReadFeatures,
					HuffmanIntegerEncoding.toParam(calculator.values(),
							calculator.bitLens()));
		}

		{ // feature position
			IntegerEncodingCalculator calc = new IntegerEncodingCalculator(
					"read feature position");
			for (CramRecord r : records) {
				int prevPos = 0;
				if (r.getReadFeatures() == null)
					continue;
				for (ReadFeature rf : r.getReadFeatures()) {
					calc.addValue(rf.getPosition() - prevPos);
					prevPos = rf.getPosition();
				}
			}

			Encoding<Integer> bestEncoding = calc.getBestEncoding();
			h.eMap.put(EncodingKey.FP_FeaturePosition, new EncodingParams(
					bestEncoding.id(), bestEncoding.toByteArray()));
		}

		{ // feature code
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				if (r.getReadFeatures() == null)
					continue;
				else
					for (ReadFeature rf : r.getReadFeatures())
						calculator.add(rf.getOperator());
			calculator.calculate();

			h.eMap.put(EncodingKey.FC_FeatureCode, HuffmanByteEncoding.toParam(
					calculator.valuesAsBytes(), calculator.bitLens));
		}

		{ // bases:
			h.eMap.put(EncodingKey.BA_Base,
					ExternalByteEncoding.toParam(baseID));
		}

		{ // quality scores:
			// HuffmanParamsCalculator calculator = new
			// HuffmanParamsCalculator();
			// for (CramRecord r : records) {
			// if (r.getQualityScores() == null) {
			// if (r.getReadFeatures() != null) {
			// for (ReadFeature f:r.getReadFeatures()) {
			// switch (f.getOperator()) {
			// case BaseQualityScore.operator:
			// calculator.add(((BaseQualityScore)f).getQualityScore()) ;
			// break;
			// default:
			// break;
			// }
			// }
			// }
			// } else {
			// for (byte s:r.getQualityScores()) calculator.add(s) ;
			// }
			// }
			// calculator.calculate();
			//
			// h.eMap.put(EncodingKey.QS_QualityScore,
			// HuffmanByteEncoding.toParam(
			// calculator.valuesAsBytes(), calculator.bitLens));

			h.eMap.put(EncodingKey.QS_QualityScore,
					ExternalByteEncoding.toParam(qualityScoreID));
		}

		{ // base substitution code
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				if (r.getReadFeatures() == null)
					continue;
				else
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
			// HuffmanParamsCalculator calculator = new
			// HuffmanParamsCalculator();
			// for (CramRecord r : records)
			// calculator.add(r.getReadName().length());
			// for (CramRecord r : records)
			// if (r.getReadFeatures() == null)
			// continue;
			// else
			// for (ReadFeature rf : r.getReadFeatures()) {
			// if (rf.getOperator() == InsertionVariation.operator)
			// calculator.add(((InsertionVariation) rf)
			// .getSequence().length);
			// if (rf.getOperator() == SoftClipVariation.operator)
			// calculator.add(((SoftClipVariation) rf)
			// .getSequence().length);
			// }
			//
			// calculator.calculate();
			//
			// h.eMap.put(EncodingKey.IN_Insertion,
			// ByteArrayLenEncoding.toParam(
			// HuffmanIntegerEncoding.toParam(calculator.values(),
			// calculator.bitLens()), ExternalByteArrayEncoding
			// .toParam(baseID)));
			h.eMap.put(EncodingKey.IN_Insertion,
					ByteArrayStopEncoding.toParam((byte) 0, baseID));
		}

		{ // deletion length
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				if (r.getReadFeatures() == null)
					continue;
				else
					for (ReadFeature rf : r.getReadFeatures())
						if (rf.getOperator() == DeletionVariation.operator)
							calculator
									.add(((DeletionVariation) rf).getLength());
			calculator.calculate();

			h.eMap.put(EncodingKey.DL_DeletionLength, HuffmanIntegerEncoding
					.toParam(calculator.values, calculator.bitLens));
		}

		{ // mapping quality score
			HuffmanParamsCalculator calculator = new HuffmanParamsCalculator();
			for (CramRecord r : records)
				if (!r.segmentUnmapped)
					calculator.add(r.getMappingQuality());
			calculator.calculate();

			h.eMap.put(EncodingKey.MQ_MappingQualityScore,
					HuffmanIntegerEncoding.toParam(calculator.values(),
							calculator.bitLens));
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

		{ // test mark
			// h.eMap.put(EncodingKey.TM_TestMark,
			// BetaIntegerEncoding.toParam(0, 32));
		}

		return h;
	}

	private static class MutableInt {
		public int value = 0;
	}

	private static class BitCode implements Comparable<BitCode> {
		int value;
		int len;

		public BitCode(int value, int len) {
			this.value = value;
			this.len = len;
		}

		@Override
		public int compareTo(BitCode o) {
			int result = value - o.value;
			if (result != 0)
				return result;
			return len - o.len;
		}
	}

	public static class HuffmanParamsCalculator {
		private HashMap<Integer, MutableInt> countMap = new HashMap<Integer, MutableInt>();
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

		public void add(Integer value, int inc) {
			MutableInt counter = countMap.get(value);
			if (counter == null) {
				counter = new MutableInt();
				countMap.put(value, counter);
			}
			counter.value += inc;
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

			List<Integer> valueList = new ArrayList<Integer>();
			List<Integer> lens = new ArrayList<Integer>();
			HuffmanCode.getValuesAndBitLengths(valueList, lens, tree);

			// the following sorting is not really required, but whatever:
			BitCode[] codes = new BitCode[valueList.size()];
			for (int i = 0; i < valueList.size(); i++) {
				codes[i] = new BitCode(valueList.get(i), lens.get(i));
			}
			Arrays.sort(codes);

			values = new int[codes.length];
			bitLens = new int[codes.length];

			for (int i = 0; i < codes.length; i++) {
				BitCode code = codes[i];
				bitLens[i] = code.len;
				values[i] = code.value;
			}
		}
	}

	private static class EncodingLengthCalculator {
		private BitCodec<Integer> codec;
		private Encoding<Integer> encoding;
		private long len;

		public EncodingLengthCalculator(Encoding<Integer> encoding) {
			this.encoding = encoding;
			codec = encoding.buildCodec(null, null);
		}

		public void add(int value) {
			len += codec.numberOfBits(value);
		}

		public void add(int value, int inc) {
			len += inc * codec.numberOfBits(value);
		}

		public long len() {
			return len;
		}
	}

	private static class IntegerEncodingCalculator {
		private List<EncodingLengthCalculator> calcs = new ArrayList<EncodingLengthCalculator>();
		private int max = 0;
		private int count = 0;
		private String name;
		private HashMap<Integer, MutableInt> dictionary = new HashMap<Integer, MutableInt>();
		private int dictionaryThreshold = 100;

		public IntegerEncodingCalculator(String name, int dictionaryThreshold) {
			this.name = name;
//			for (int i = 2; i < 10; i++)
//				calcs.add(new EncodingLengthCalculator(
//						new GolombIntegerEncoding(i)));
//
//			for (int i = 2; i < 20; i++)
//				calcs.add(new EncodingLengthCalculator(
//						new GolombRiceIntegerEncoding(i)));

			calcs.add(new EncodingLengthCalculator(new GammaIntegerEncoding(1)));

			for (int i = 2; i < 5; i++)
				calcs.add(new EncodingLengthCalculator(
						new SubexpIntegerEncoding(i)));

			if (dictionaryThreshold < 1)
				dictionary = null;
			else {
				dictionary = new HashMap<Integer, MutableInt>();
//				int pow = (int) Math.ceil(Math.log(dictionaryThreshold)
//						/ Math.log(2f));
//				dictionaryThreshold = 1 << pow ;
//				dictionary = new HashMap<Integer, MutableInt>(dictionaryThreshold, 1);
			}
		}

		public IntegerEncodingCalculator(String name) {
			this(name, 255);
		}

		public void addValue(int value) {
			count++;
			if (value > max)
				max = value;

			for (EncodingLengthCalculator c : calcs)
				c.add(value);

			if (dictionary != null) {
				if (dictionary.size() >= dictionaryThreshold - 1)
					dictionary = null;
				else {
					MutableInt m = dictionary.get(value);
					if (m == null) {
						m = new MutableInt();
						dictionary.put(value, m);
					}
					m.value++;
				}

			}

		}

		public Encoding<Integer> getBestEncoding() {
			EncodingLengthCalculator bestC = calcs.get(0);

			for (EncodingLengthCalculator c : calcs) {
				if (c.len() < bestC.len())
					bestC = c;
			}

			Encoding<Integer> bestEncoding = bestC.encoding;
			long bits = bestC.len();

			{ // check if beta is better:

				int betaLength = (int) Math.round(Math.log(max) / Math.log(2)
						+ 0.5);
				if (bits > betaLength * count) {
					bestEncoding = new BetaIntegerEncoding(betaLength);
					bits = betaLength * count;
				}
			}

			{ // try huffman:
				if (dictionary != null) {
					HuffmanParamsCalculator c = new HuffmanParamsCalculator();
					for (Integer value : dictionary.keySet())
						c.add(value, dictionary.get(value).value);

					c.calculate();

					EncodingParams param = HuffmanIntegerEncoding.toParam(
							c.values(), c.bitLens());
					HuffmanIntegerEncoding he = new HuffmanIntegerEncoding();
					he.fromByteArray(param.params);
					EncodingLengthCalculator lc = new EncodingLengthCalculator(
							he);
					for (Integer value : dictionary.keySet())
						lc.add(value, dictionary.get(value).value);

					if (lc.len() < bits) {
						bestEncoding = he;
						bits = lc.len();
					}
				}
			}

			byte[] params = bestEncoding.toByteArray();
			params = Arrays.copyOf(params, Math.min(params.length, 20));
			log.debug("Best encoding for " + name + ": "
					+ bestEncoding.id().name() + Arrays.toString(params));

			return bestEncoding;
		}
	}
}
