package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;

public class Codec {

	public static ByteBuffer uncompress(ByteBuffer in) {
		int order = in.get();

		switch (order) {
		case 0:
			return D04.decode(in);

		case 1:
			return D14.decode(in.slice(), null);

		default:
			throw new RuntimeException("Unknown rANS order: " + order);
		}
	}

	public static ByteBuffer compress(ByteBuffer in, int order) {
		System.err.println("rans codec, order " + order);
		switch (order) {
		case 0:
			return E04.encode(in, null);
		case 1:
			return E14.encode(in.slice(), null);

		default:
			throw new RuntimeException("Unknown rANS order: " + order);
		}
	}
}
