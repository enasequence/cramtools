package net.sf.block;

import java.nio.ByteBuffer;

public class ByteBufferUtils {
	
	public static final int readUnsignedITF8 (byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data) ;
		int value = ByteBufferUtils.readUnsignedITF8(buf) ;
		buf.clear() ;
		
		return value ;
	}
	
	public static final byte[] writeUnsignedITF8 (int value) {
		ByteBuffer buf = ByteBuffer.allocate(10);
		ByteBufferUtils.writeUnsignedITF8(value, buf);
		
		buf.flip();
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		
		buf.clear() ;
		return array;
	}

	public static final int readUnsignedITF8(ByteBuffer buf) {
		int b1 = 0xFF & buf.get();

		if ((b1 & 0b10000000) == 0)
			return b1;

		if ((b1 & 0b01000000) == 0)
			return ((b1 & 0b01111111) << 8) | (0xFF & buf.get());

		if ((b1 & 0b00100000) == 0) {
			int b2 = 0xFF & buf.get() ;
			int b3 = 0xFF & buf.get() ;
			return ((b1 & 0b00111111) << 16) | b2 << 8
					| b3;
		}

		if ((b1 & 0b00010000) == 0)
			return ((b1 & 0b00011111) << 24) | (0xFF & buf.get()) << 16
					| (0xFF & buf.get()) << 8 | (0xFF & buf.get());

		return ((b1 & 0b00001111) << 28) | (0xFF & buf.get()) << 20
				| (0xFF & buf.get()) << 12 | (0xFF & buf.get()) << 4
				| (0b00001111 & buf.get());
	}

	public static final void writeUnsignedITF8(int value, ByteBuffer buf) {
		if ((value >>> 7) == 0) {
			buf.put((byte) value);
			return;
		}

		if ((value >>> 14) == 0) {
			buf.put((byte) ((value >> 8) | 0b10000000));
			buf.put((byte) (value & 0xFF));
			return;
		}

		if ((value >>> 21) == 0) {
			buf.put((byte) ((value >> 16) | 0b11000000));
			buf.put((byte) ((value >> 8) & 0xFF));
			buf.put((byte) (value & 0xFF));
			return;
		}

		if ((value >>> 28) == 0) {
			buf.put((byte) ((value >> 24) | 0b11100000));
			buf.put((byte) ((value >> 16) & 0xFF));
			buf.put((byte) ((value >> 8) & 0xFF));
			buf.put((byte) (value & 0xFF));
			return;
		}

		buf.put((byte) ((value >> 28) | 0b11110000));
		buf.put((byte) ((value >> 20) & 0xFF));
		buf.put((byte) ((value >> 12) & 0xFF));
		buf.put((byte) ((value >> 4) & 0xFF));
		buf.put((byte) (value & 0xFF));
	}

	public static void main(String[] args) {
		ByteBuffer buf = ByteBuffer.allocate(5);
		
		//Read 192 but expecting 16384
		writeUnsignedITF8(16384, buf) ;
		buf.flip() ;
		int v = readUnsignedITF8(buf) ;
		System.out.println(v);
		
		long time=System.nanoTime() ;
		for (int i=0; i<Integer.MAX_VALUE; i++) {
			buf.clear() ;
			writeUnsignedITF8(i, buf);
			buf.flip() ;
			int value = readUnsignedITF8(buf) ; 
			if (i != value)
				throw new RuntimeException("Read " + value + " but expecting " + i);
			
			if (System.nanoTime()-time > 1000*1000*1000) {
				time = System.nanoTime() ;
				System.out.println("i=" + i);
			}
		}
		
		System.out.println("Done.");
	}
}
