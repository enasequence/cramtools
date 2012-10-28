package net.sf.cram.encoding.read_features;

import java.io.IOException;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class SubstitutionVariationCodec implements
		BitCodec<SubstitutionVariation> {
	public BitCodec<BaseChange> baseChangeCodec;
//	public BitCodec<Byte> qualityScoreCodec;

	@Override
	public SubstitutionVariation read(BitInputStream bis) throws IOException {
		SubstitutionVariation v = new SubstitutionVariation();
		// position is not read here because we need to keep track of previous
		// values read from the codec. See ReadFeatureCodec.
		long position = -1L;
		BaseChange baseChange = baseChangeCodec.read(bis);
//		byte qualityScore = qualityScoreCodec.read(bis);

		v.setPosition((int) position);
		v.setBaseChange(baseChange);
//		v.setQualityScore(qualityScore);

		return v;
	}

	@Override
	public long write(BitOutputStream bos, SubstitutionVariation v)
			throws IOException {
		long len = 0L;

		BaseChange baseChange = v.getBaseChange();
		if (baseChange == null)
			baseChange = new BaseChange(v.getRefernceBase(), v.getBase());
		
		long baseChangeLen = 0L ;
		baseChangeLen-= len ;
		len += baseChangeCodec.write(bos, baseChange);
		baseChangeLen+= len ;

//		len += qualityScoreCodec.write(bos, v.getQualityScore());
		
		return len;
	}

	@Override
	public long numberOfBits(SubstitutionVariation v) {
		try {
			return write(NullBitOutputStream.INSTANCE, v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SubstitutionVariation read(BitInputStream bis, int len)
			throws IOException {
		throw new RuntimeException("Not implemented,") ;
	}

}
