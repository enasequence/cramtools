package net.sf.cram.encoding.huffint;

import net.sf.cram.io.IOUtils;

class HuffmanBitCode {
	int bitCode;
	int bitLentgh;
	int value;
	
	@Override
	public String toString() {
		return value + ":\t" + IOUtils.toBitString(bitCode).substring(32-bitLentgh)  + " " + bitCode;
	}
}