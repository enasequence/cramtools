package net.sf.cram.encoding.huffint;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.cram.encoding.AbstractBitCodec;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.BitOutputStream;


public class CanonicalHuffmanByteCodec2 extends AbstractBitCodec<Byte> {
	private final HelperByte helper ;

	/*
	 * values[]: the alphabet (provided as Integers) bitLengths[]: the number of
	 * bits of symbil's huffman code
	 */
	public CanonicalHuffmanByteCodec2(byte[] values, int[] bitLengths) {
		helper = new HelperByte(values, bitLengths) ;
	}
	
	@Override
	public Byte read(BitInputStream bis) throws IOException {
		return helper.read(bis) ;
	}

	@Override
	public long write(BitOutputStream bos, Byte object) throws IOException {
		return helper.write(bos, object) ;
	}

	@Override
	public long numberOfBits(Byte object) {
		HuffmanBitCode bitCode;
		try {
			bitCode = helper.codes.get(object);
			return bitCode.bitLentgh ;
		} catch (NullPointerException e) {
			throw new RuntimeException("Value " + object + " not found.", e) ;
		}
	}
	
	@Override
	public Byte read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented");
	}
}
