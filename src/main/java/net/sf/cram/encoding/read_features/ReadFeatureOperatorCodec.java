package net.sf.cram.encoding.read_features;

import uk.ac.ebi.ena.sra.compression.huffman.HuffmanCode;
import uk.ac.ebi.ena.sra.cram.encoding.HuffmanCodec;

public class ReadFeatureOperatorCodec extends HuffmanCodec<Byte> {

	public ReadFeatureOperatorCodec() {
		this(new int[] { 100, 50, 10, 5 }, new Byte[] { 'N', 'S', 'I', 'D' });
	}

	public ReadFeatureOperatorCodec(int[] frequencies, Byte[] alphabet) {
		super(HuffmanCode.buildTree(frequencies, alphabet));
	}

}
