package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import uk.ac.ebi.ena.sra.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.encoding.ByteArrayBitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

public class BALengthEncoding implements Encoding<byte[]>{
	public Encoding<Integer> lenEncoding ;
	public Encoding<Byte> byteEncoding ;

	@Override
	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromByteArray(byte[] data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BitCodec<byte[]> buildCodec(Map<Short, InputStream> inputMap,
			Map<Short, OutputStream> outputMap) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteArrayBitCodec buildByteArrayCodec(
			Map<Short, InputStream> inputMap, Map<Short, OutputStream> outputMap) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static class BALengthCodec implements ByteArrayBitCodec {

		@Override
		public String getName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public byte[] read(BitInputStream bis, int len) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long write(BitOutputStream bos, byte[] object)
				throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long numberOfBits(byte[] object) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Stats getStats() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

}
