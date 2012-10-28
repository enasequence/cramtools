package net.sf.cram.encoding.read_features;

import java.io.IOException;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class DeletionVariationCodec implements BitCodec<DeletionVariation> {
	public BitCodec<Long> dellengthPosCodec;

	@Override
	public DeletionVariation read(BitInputStream bis) throws IOException {
		// position is not read here because we need to keep track of previous
		// values read from the codec. See ReadFeatureCodec.
		long position = -1L;
		long length = dellengthPosCodec.read(bis);

		DeletionVariation v = new DeletionVariation();
		v.setPosition((int) position);
		v.setLength((int) length);
		return v;
	}

	@Override
	public long write(BitOutputStream bos, DeletionVariation v)
			throws IOException {
		long len = 0L;

		len += dellengthPosCodec.write(bos, (long) v.getLength());

		return len;
	}

	@Override
	public long numberOfBits(DeletionVariation v) {
		try {
			return write(NullBitOutputStream.INSTANCE, v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
