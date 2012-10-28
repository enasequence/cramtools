package net.sf.cram.encoding.read_features;

import java.io.IOException;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

public class BaseChangeCodec implements BitCodec<BaseChange> {

	@Override
	public BaseChange read(BitInputStream bis) throws IOException {
		return new BaseChange(bis.readBits(2));
	}

	@Override
	public long write(BitOutputStream bis, BaseChange baseChange) throws IOException {
		bis.write(baseChange.getChange(), 2);
		return 2;
	}

	@Override
	public long numberOfBits(BaseChange baseChange) {
		return 2;
	}

	@Override
	public BaseChange read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented,") ;
	}

}
