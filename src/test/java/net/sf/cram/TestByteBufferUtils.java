package net.sf.cram;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;

import net.sf.cram.io.ByteBufferUtils;

import org.junit.Assert;
import org.junit.Test;

public class TestByteBufferUtils {

	@Test
	public void test1() {
		java.util.List<Integer> list = new ArrayList<Integer>();

		list.add(ReadTag.nameType3BytesToInt("OQ", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("X0", 'C'));
		list.add(ReadTag.nameType3BytesToInt("X0", 'c'));
		list.add(ReadTag.nameType3BytesToInt("X0", 's'));
		list.add(ReadTag.nameType3BytesToInt("X1", 'C'));
		list.add(ReadTag.nameType3BytesToInt("X1", 'c'));
		list.add(ReadTag.nameType3BytesToInt("X1", 's'));
		list.add(ReadTag.nameType3BytesToInt("XA", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("XC", 'c'));
		list.add(ReadTag.nameType3BytesToInt("XT", 'A'));
		list.add(ReadTag.nameType3BytesToInt("OP", 'i'));
		list.add(ReadTag.nameType3BytesToInt("OC", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("BQ", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("AM", 'c'));

		int maxRecords = 100000;
		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 10);

		long writeNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (Integer value : list) {
				long time = System.nanoTime();
				ByteBufferUtils.writeUnsignedITF8(value, buf);
				writeNanos += System.nanoTime() - time;
			}
		}

		buf.flip();
		long readNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (Integer value : list) {
				long time = System.nanoTime();
				int readValue = ByteBufferUtils.readUnsignedITF8(buf);
				readNanos += System.nanoTime() - time;
				if (value.intValue() != readValue)
					fail("Failed to read value: " + value + ", read instead "
							+ readValue);
			}
		}

		System.out
				.printf("ByteBufferUtils: buf size %.2f megabytes, write time %dms, read time %dms.\n",
						buf.limit() / 1024f / 1024f, writeNanos / 1000000,
						readNanos / 1000000);
	}

	@Test
	public void test2() {
		java.util.List<Integer> list = new ArrayList<Integer>();

		list.add(ReadTag.nameType3BytesToInt("OQ", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("X0", 'C'));
		list.add(ReadTag.nameType3BytesToInt("X0", 'c'));
		list.add(ReadTag.nameType3BytesToInt("X0", 's'));
		list.add(ReadTag.nameType3BytesToInt("X1", 'C'));
		list.add(ReadTag.nameType3BytesToInt("X1", 'c'));
		list.add(ReadTag.nameType3BytesToInt("X1", 's'));
		list.add(ReadTag.nameType3BytesToInt("XA", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("XC", 'c'));
		list.add(ReadTag.nameType3BytesToInt("XT", 'A'));
		list.add(ReadTag.nameType3BytesToInt("OP", 'i'));
		list.add(ReadTag.nameType3BytesToInt("OC", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("BQ", 'Z'));
		list.add(ReadTag.nameType3BytesToInt("AM", 'c'));

		int maxRecords = 100000;
		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 10);
		buf.order(ByteOrder.LITTLE_ENDIAN);

		long writeNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (Integer value : list) {
				long time = System.nanoTime();
				buf.putInt(value);
				writeNanos += System.nanoTime() - time;
			}
		}

		buf.flip();
		long readNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (Integer value : list) {
				long time = System.nanoTime();
				int readValue = buf.getInt();
				readNanos += System.nanoTime() - time;
				if (value.intValue() != readValue)
					fail("Failed to read value: " + value + ", read instead "
							+ readValue);
			}
		}

		System.out
				.printf("ByteBuffer: buf size %.2f megabytes, write time %dms, read time %dms.\n",
						buf.limit() / 1024f / 1024f, writeNanos / 1000000,
						readNanos / 1000000);
	}

	@Test
	public void test3() {
		java.util.List<byte[]> list = new ArrayList<byte[]>();

		list.add("OQZ".getBytes());
		list.add("X0C".getBytes());
		list.add("X0c".getBytes());
		list.add("X0s".getBytes());
		list.add("X1C".getBytes());
		list.add("X1c".getBytes());
		list.add("X1s".getBytes());
		list.add("XAZ".getBytes());
		list.add("XCc".getBytes());
		list.add("XTA".getBytes());
		list.add("OPi".getBytes());
		list.add("OCZ".getBytes());
		list.add("BQZ".getBytes());
		list.add("AMC".getBytes());

		int maxRecords = 100000;
		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 10);
		buf.order(ByteOrder.LITTLE_ENDIAN);

		long writeNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (byte[] value : list) {
				long time = System.nanoTime();
				buf.put(value);
				writeNanos += System.nanoTime() - time;
			}
		}

		buf.flip();
		long readNanos = 0;
		for (int i = 0; i < maxRecords; i++) {
			for (byte[] value : list) {
				long time = System.nanoTime();
				byte[] bytes = new byte[3];
				buf.get(bytes);
				readNanos += System.nanoTime() - time;
				Assert.assertArrayEquals(value, bytes);
			}
		}

		System.out
				.printf("Direct bytes: buf size %.2f megabytes, write time %dms, read time %dms.\n",
						buf.limit() / 1024f / 1024f, writeNanos / 1000000,
						readNanos / 1000000);
	}
	
	@Test
	public void test4() {
		int value = ByteBufferUtils.readUnsignedITF8(new byte[]{-127, 8}) ;
		System.out.println(value);
		
		System.out.println(Arrays.toString(ByteBufferUtils.writeUnsignedITF8(value)));
	}

}
