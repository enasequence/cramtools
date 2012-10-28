package net.sf.cram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.cram.CompressionHeader;
import net.sf.cram.CramRecord;
import net.sf.cram.EncodingKey;
import net.sf.cram.Utils;
import net.sf.cram.encoding.ExternalByteEncoding;
import net.sf.cram.encoding.GolombEncoding;
import net.sf.cram.encoding.HuffmanEncoding;
import net.sf.cram.encoding.NullEncoding;
import uk.ac.ebi.ena.sra.compression.huffman.HuffmanCode;
import uk.ac.ebi.ena.sra.compression.huffman.HuffmanTree;
import uk.ac.ebi.ena.sra.cram.io.IOUtils;

public class CompressionHeaderFactory {

	public CompressionHeader build(List<CramRecord> records) {
		CompressionHeader h = new CompressionHeader();
		h.eMap = new TreeMap<>();
		for (EncodingKey key : EncodingKey.values())
			h.eMap.put(key, NullEncoding.toParam());
		
		h.tMap = new TreeMap<>();

		h.eMap.put(EncodingKey.BA_Base, ExternalByteEncoding.toParam(1));
		h.eMap.put(EncodingKey.QS_QualityScore, ExternalByteEncoding.toParam(2));
		h.eMap.put(EncodingKey.RN_ReadName, ExternalByteEncoding.toParam(3));

		h.eMap.put(EncodingKey.AP_AlignmentPosition, GolombEncoding.toParam(5));

		int[] bfValues, bfBitLens;
		{
			HuffmanIntParamsBuilder huffmanIntParamsBuilder = new HuffmanIntParamsBuilder();
			for (CramRecord r : records)
				huffmanIntParamsBuilder.add(r.getFlags());
			HuffmanIntParams params = huffmanIntParamsBuilder.getParams();
			bfValues = params.bfValues;
			bfBitLens = params.bfBitLens;
		}
		h.eMap.put(EncodingKey.BF_BitFlags,
				HuffmanEncoding.toParam(bfValues, bfBitLens));

		return h;
	}

	private static class HuffmanIntParams {
		int[] bfValues = new int[] {};
		int[] bfBitLens = new int[] {};
	}

	private static class MutableInt {
		public int value = 0;
	}

	private static class HuffmanIntParamsBuilder {
		HashMap<Integer, MutableInt> map = new HashMap<>();

		public void add(int huffmanValue) {
			MutableInt counter = map.get(huffmanValue);
			if (counter == null) {
				counter = new MutableInt();
				map.put(huffmanValue, counter);
			}
			counter.value++;
		}

		public HuffmanIntParams getParams() {
			HuffmanIntParams p = new HuffmanIntParams();
			HuffmanTree<Integer> tree = null;
			{
				int size = map.size();
				int[] freqs = new int[size];
				int[] values = new int[size];

				int i = 0;
				for (Integer v : map.keySet()) {
					values[i] = v;
					freqs[i] = map.get(v).value;
					i++;
				}
				tree = HuffmanCode.buildTree(freqs, Utils.autobox(values));
			}

			List<Integer> valueList = new ArrayList<>();
			List<Integer> lens = new ArrayList<>();
			HuffmanCode.getValuesAndBitLengths(valueList, lens, tree);

			p.bfValues = new int[valueList.size()];
			p.bfBitLens = new int[valueList.size()];

			for (int i = 0; i < p.bfBitLens.length; i++) {
				p.bfBitLens[i] = lens.get(i);
				p.bfValues[i] = valueList.get(i);
			}

			return p;
		}
	}
}
