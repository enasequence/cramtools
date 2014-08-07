package net.sf.cram.encoding.rANS;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.sf.cram.io.ByteBufferUtils;

public class rANS {

	public static ByteBuffer uncompress(ByteBuffer in) {
		in.order(ByteOrder.LITTLE_ENDIAN);
		int order = in.get();
		// int size = in.getInt();

		switch (order) {
		case 0:
			return new rANS_decode0_4way().rans_uncompress_O0(in);

		case 1:
			return new rANS_Decoder1_4way().rans_uncompress_O1(in.slice(), null);

		default:
			throw new RuntimeException("Unknown rANS order: " + order);
		}
	}

	public static void main(String[] args) throws IOException {
		InputStream is = rANS.class.getClassLoader().getResourceAsStream("data/test.rans1");
		byte[] bytes = ByteBufferUtils.readFully(is);
		ByteBuffer buf = uncompress(ByteBuffer.wrap(bytes));
		System.out.println(ByteBufferUtils.substring(buf, 20));
	}
}
