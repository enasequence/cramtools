package net.sf.cram.encoding.read_features;

import java.io.IOException;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class InsertionVariationCodec implements BitCodec<InsertionVariation> {
	public BitCodec<byte[]> insertBasesCodec;
	private long byteCounter = 0 ;
	private long totalLen = 0 ;

	@Override
	public InsertionVariation read(BitInputStream bis) throws IOException {
		// position is not read here because we need to keep track of previous
		// values read from the codec. See ReadFeatureCodec.
		long position = -1L;
		byte[] insertion = insertBasesCodec.read(bis);

		InsertionVariation v = new InsertionVariation();
		v.setPosition((int) position);
		v.setSequence(insertion);
		return v;
	}

	@Override
	public long write(BitOutputStream bos, InsertionVariation v)
			throws IOException {
		long len = 0L;

		len += insertBasesCodec.write(bos, v.getSequence());
//		System.out.println(new String(v.getSequence()) + " encoded in " +  len + " bits.");
		
		byteCounter += v.getSequence().length ;
		totalLen += len ;

		return len;
	}

	@Override
	public long numberOfBits(InsertionVariation v) {
		try {
			return write(NullBitOutputStream.INSTANCE, v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("Insertion codec: %d bases total, %d bits, %.2f bits per base.", byteCounter, totalLen, (float)totalLen/byteCounter) ;
	}

}
