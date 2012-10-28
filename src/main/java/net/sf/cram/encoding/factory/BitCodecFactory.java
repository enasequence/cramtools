package net.sf.cram.encoding.factory;

import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.Encoding;
import uk.ac.ebi.ena.sra.cram.encoding.ByteArrayBitCodec;

public interface BitCodecFactory {

	public BitCodec<Long> buildLongCodec(Encoding encoding);

	public BitCodec<Integer> buildIntegerCodec(Encoding encoding);

	public BitCodec<Byte> buildByteCodec(Encoding encoding);

	public BitCodec<byte[]> buildByteArrayCodec(Encoding encoding);

	public ByteArrayBitCodec buildByteArrayCodec2(Encoding encoding);

}