package net.sf.cram.encoding.rans;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import net.sf.cram.encoding.rans.RANS.ORDER;

import org.junit.Test;

public class TestRansCodec {

	@Test
	public void testEmpty() {
		roundTrip(new byte[0]);
	}

	@Test
	public void testOneByte() {
		roundTrip(new byte[] { 0 });
	}

	@Test
	public void testTwoByte() {
		roundTrip(new byte[] { 0, 1 });
	}

	@Test
	public void testThreeByte() {
		roundTrip(new byte[] { 0, 1, 2 });
	}

	@Test
	public void testFourBytes() {
		roundTrip(new byte[] { 0, 1, 2, 3 });
	}

	@Test
	public void testByteRange() {
		byte[] data = new byte[256];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) i;
		roundTrip(data);
	}

	@Test
	public void testZeroBytes() {
		roundTrip(new byte[1000]);
	}

	@Test
	public void testUniBytes() {
		byte[] data = new byte[1000];
		Arrays.fill(data, (byte) 1);
		roundTrip(data);
	}

	@Test
	public void testZeroOneStretches() {
		byte[] data = new byte[1000];
		Arrays.fill(data, data.length / 2, data.length, (byte) 1);
		roundTrip(data);
	}

	@Test
	public void testByteMin() {
		byte[] data = new byte[1000];
		Arrays.fill(data, Byte.MIN_VALUE);
		roundTrip(data);
	}

	@Test
	public void testByteMax() {
		byte[] data = new byte[1000];
		Arrays.fill(data, Byte.MAX_VALUE);
		roundTrip(data);
	}

	@Test
	public void test1000_0dot1() {
		roundTrip(randomBytes_GD(1000, 0.1));
	}

	@Test
	public void test1000_0dot01() {
		roundTrip(randomBytes_GD(1000, 0.01));
	}

	@Test
	public void testSizeRange_tiny() {
		for (int i = 0; i < 20; i++) {
			byte[] data = randomBytes_GD(100, 0.1);
			ByteBuffer in = ByteBuffer.wrap(data);
			for (int size = 1; size < data.length; size++) {
				in.position(0);
				in.limit(size);
				try {
					roundTrip(in);
				} catch (AssertionError e) {
					System.err.printf("Failed at size %d and data %s\n", size, Arrays.toString(data));
					throw e;
				}
			}
		}
	}

	@Test
	public void testSizeRange_small() {
		byte[] data = randomBytes_GD(1000, 0.01);
		ByteBuffer in = ByteBuffer.wrap(data);
		for (int size = 4; size < data.length; size++) {
			in.position(0);
			in.limit(size);
			roundTrip(in);
		}
	}

	@Test
	public void testLargeSize() {
		int size = 100 * 1000 + 3;
		byte[] data = randomBytes_GD(size, 0.01);
		ByteBuffer in = ByteBuffer.wrap(data);
		for (int limit = size - 4; limit < size; limit++) {
			in.position(0);
			in.limit(limit);
			roundTrip(in);
		}
	}

	@Test
	public void testXLSize() {
		int size = 10 * 1000 * 1000 + 1;
		roundTrip(randomBytes_GD(size, 0.01));
	}

	@Test
	public void testBuffersMeetBoundaryExpectations() {
		int size = 1001;
		ByteBuffer raw = ByteBuffer.wrap(randomBytes_GD(size, 0.01));
		for (ORDER order : ORDER.values()) {
			ByteBuffer compressed = RANS.compress(raw, order, null);
			assertFalse(raw.hasRemaining());
			assertThat(raw.limit(), is(size));

			assertThat(compressed.position(), is(0));
			assertTrue(compressed.limit() > 10);
			assertThat(compressed.get(), is((byte) order.ordinal()));
			assertThat(compressed.getInt(), is(compressed.limit() - 1 - 4 - 4));
			assertThat(compressed.getInt(), is(size));
			compressed.rewind();

			ByteBuffer uncompressed = RANS.uncompress(compressed, null);
			assertFalse(compressed.hasRemaining());
			assertThat(uncompressed.limit(), is(size));
			assertThat(uncompressed.position(), is(0));

			raw.rewind();
		}
	}

	@Test
	public void testRansHeader() {
		byte[] data = randomBytes_GD(1000, 0.01);
		ByteBuffer compressed = RANS.compress(ByteBuffer.wrap(data), ORDER.ZERO, null);
		assertThat(compressed.get(), is((byte) 0));
		assertThat(compressed.getInt(), is(compressed.limit() - 9));
		assertThat(compressed.getInt(), is(data.length));
	}

	private static void roundTrip(ByteBuffer data) {
		for (ORDER order : ORDER.values()) {
			roundTrip(data, order);
			data.rewind();
		}
	}

	private static void roundTrip(byte[] data) {
		for (ORDER order : ORDER.values())
			roundTrip(data, order);
	}

	private static void roundTrip(ByteBuffer data, ORDER order) {
		ByteBuffer compressed = RANS.compress(data, order, null);
		ByteBuffer uncompressed = RANS.uncompress(compressed, null);
		data.rewind();
		while (data.hasRemaining()) {
			if (!uncompressed.hasRemaining())
				fail("Premature end of uncompressed data.");
			assertThat(uncompressed.get(), is(data.get()));
		}
		assertFalse(uncompressed.hasRemaining());
	}

	private static void roundTrip(byte[] data, ORDER order) {
		roundTrip(ByteBuffer.wrap(data), order);
	}

	private Random random = new Random();

	private byte[] randomBytes_GD(int size, double p) {
		byte[] data = new byte[size];
		randomBytes_GD(data, p);
		return data;
	}

	private void randomBytes_GD(byte[] data, double p) {
		for (int i = 0; i < data.length; i++)
			data[i] = randomByteGeometricDistribution(p);
	}

	/**
	 * A crude implementation of RNG for sampling geometric distribution. The
	 * value returned is offset by -1 to include zero. For testing purposes
	 * only, no refunds!
	 * 
	 * @param p
	 *            the probability of success
	 * @return an almost random byte value.
	 */
	private byte randomByteGeometricDistribution(double p) {
		double rand = random.nextDouble();
		double g = Math.ceil(Math.log(1 - rand) / Math.log(1 - p)) - 1;
		return (byte) g;
	}
}
