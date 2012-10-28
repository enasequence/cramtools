package net.sf.cram.encoding.factory;

import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.Encoding;
import uk.ac.ebi.ena.sra.cram.encoding.ByteArrayBitCodec;

public class DefaultBitCodecFactory implements BitCodecFactory{

	@Override
	public BitCodec<Long> buildLongCodec(Encoding encoding) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitCodec<Integer> buildIntegerCodec(Encoding encoding) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitCodec<Byte> buildByteCodec(Encoding encoding) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitCodec<byte[]> buildByteArrayCodec(Encoding encoding) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteArrayBitCodec buildByteArrayCodec2(Encoding encoding) {
		
		return null;
	}

}
