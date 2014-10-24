package net.sf.cram.encoding.rans2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

class TestCodec {

	private static byte[] generateRandomData(int size) {
		byte[] data = new byte[size];
		Random random = new Random();
		for (int i = 0; i < size; i++)
			data[i] = (byte) random.nextInt(40);
		return data;
	}

	public static void main(String[] args) {
		byte[] data = generateRandomData(1000 * 1000 * 10);
		ByteBuffer inBuf = ByteBuffer.wrap(data);
		ByteBuffer compBuf = ByteBuffer.allocate(data.length * 2 + 100000);
		ByteBuffer uncBuf = ByteBuffer.allocate(data.length);
		inBuf.order(ByteOrder.LITTLE_ENDIAN);
		compBuf.order(ByteOrder.LITTLE_ENDIAN);
		uncBuf.order(ByteOrder.LITTLE_ENDIAN);

		for (int i = 0; i < 10; i++) {
			for (int order : new int[] { 0, 1 }) {
				for (int way : new int[] { 4 }) {
					inBuf.rewind();
					compBuf.clear();
					uncBuf.clear();
					test(order, way, inBuf, compBuf, uncBuf);
				}
			}
		}
	}

	private static void test(int order, int way, ByteBuffer inBuf,
			ByteBuffer compBuf, ByteBuffer uncBuf) {
		long eStart = 0, eEnd = 0, dStart = 0, dEnd = 0;
		int in_size = inBuf.remaining();

		if (order == 0) {
			eStart = System.nanoTime();
			E04.encode(inBuf, compBuf);
			eEnd = System.nanoTime();

			dStart = System.nanoTime();
			compBuf.get();
			D04.decode(compBuf);
			dEnd = System.nanoTime();
		} else {

			switch (way) {
			case 4:
				eStart = System.nanoTime();
				E14.encode(inBuf, compBuf);
				eEnd = System.nanoTime();

				dStart = System.nanoTime();
				compBuf.get();
				D14.decode(compBuf, uncBuf);
				dEnd = System.nanoTime();
				break;

			default:
				break;
			}
		}
		System.out.printf("Order %d, %d-way:\t%.2fMB/s\t%.2fMB/s.\n", order,
				way, 1000f * in_size / (eEnd - eStart), 1000f * in_size
						/ (dEnd - dStart));
	}
}
