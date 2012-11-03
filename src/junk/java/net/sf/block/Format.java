package net.sf.block;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import uk.ac.ebi.ena.sra.cram.io.ExposedByteArrayOutputStream;

public class Format {
	private Definition definition;

	public Format(Definition definition) {
		this.definition = definition;
	}

	public Definition getDefinition() {
		return definition;
	}

	public int writeBlock(Block block, OutputStream os) throws IOException {
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		block.bytes = writeData(baos, block.data, block.method);
		block.dataBytes = block.data.length;

		ExposedByteArrayOutputStream baos2 = new ExposedByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos2);
		dos.writeByte(block.contentType);
		dos.writeShort(block.contentId);
		dos.writeByte(block.method);
		dos.writeInt(block.bytes);
		dos.writeInt(block.dataBytes);

		int len = 1 + 2 + 1 + 4 + 4;
		dos.write(baos.getBuffer(), 0, baos.size());
		len += block.bytes;

		dos.close();

		os.write(baos2.getBuffer(), 0, baos2.size());

		// System.out.printf("block written: %d, %d, %d, %d, %d, %s\n",
		// block.contentType, block.contentId, block.method, block.bytes,
		// block.dataBytes, flanksToHex(block.data, 10));
		return len;
	}

	public static String toHex(byte[] data) {
		return toHex(data, 0, data.length);
	}

	public static String toHex(byte[] data, int from, int len) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < len; i++) {
			sb.append(String.format(" %02X", data[from + i]));
		}
		return sb.toString();
	}

	public static String flanksToHex(byte[] data, int flankSize) {
		return Format.toHex(data, 0, Math.min(data.length, flankSize)) + " ... "
				+ Format.toHex(data, Math.max(data.length - flankSize, 0), Math.min(data.length, flankSize));
	}

	public static void peekFlanks(String string, byte[] data) {
		System.out.println(string + Format.toHex(data, 0, Math.min(data.length, 12)) + " ... "
				+ Format.toHex(data, Math.max(data.length - 12, 0), Math.min(data.length, 12)));
	}

	public Block readBlock(InputStream is) throws IOException {
		Block block = new Block();
		DataInputStream dis = new DataInputStream(is);
		byte[] bytes = new byte[1 + 2 + 1 + 4 + 4];
		dis.readFully(bytes);
		DataInputStream dis2 = new DataInputStream(new ByteArrayInputStream(bytes));
		block.contentType = dis2.readByte();
		block.contentId = dis2.readShort();
		block.method = dis2.readByte();
		block.bytes = dis2.readInt();
		block.dataBytes = dis2.readInt();

		block.data = readData(dis, block.bytes, block.dataBytes, block.method);

		// System.out.printf("block read: %d, %d, %d, %d, %d, %s\n",
		// block.contentType, block.contentId, block.method, block.bytes,
		// block.dataBytes, flanksToHex(block.data, 10));
		return block;
	}

	public Container readContainer(InputStream is) throws IOException {
		Container c = new Container();
		DataInputStream dis = new DataInputStream(is);
		try {
			c.contentId = dis.readLong();
		} catch (EOFException e) {
			return null;
		}
		c.length = dis.readInt();

		dis = new DataInputStream(is);

		int nofBlocks = dis.readInt();
		int nofContainers = dis.readInt();

		c.blocks = new Block[nofBlocks];
		for (int i = 0; i < nofBlocks; i++) {
			c.blocks[i] = readBlock(dis);
		}

		c.containers = new Container[nofContainers];
		for (int i = 0; i < nofContainers; i++) {
			c.containers[i] = readContainer(dis);
		}
		return c;
	}

	public int writeContainer(Container c, OutputStream os) throws IOException {
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		dos.writeInt(c.blocks.length);
		dos.writeInt(c.containers.length);

		for (Block block : c.blocks)
			writeBlock((Block) block, dos);

		for (Container container : c.containers)
			writeContainer(container, dos);

		dos.flush();

		dos = new DataOutputStream(os);

		int len = 0;
		dos.writeLong(c.contentId);
		len += 8;
		dos.writeInt(baos.size());
		len += 4;

		dos.write(baos.getBuffer(), 0, baos.size());
		dos.flush();
		len += baos.size();

		baos.reset();

		return len;
	}

	private InputStream wrapInputStream(InputStream is, byte wrapperID) throws IOException {
		InputStream wrappedIS = is;
		switch (CompressionMethod.fromByte(wrapperID)) {
		case RAW:
			break;
		case GZIP:
			wrappedIS = new GZIPInputStream(is);
			break;

		default:
			throw new RuntimeException("Unknown container.");
		}
		return wrappedIS;
	}

	private OutputStream wrapOutputStream(OutputStream os, byte wrapperID) throws IOException {
		OutputStream wrappedOS = os;

		switch (CompressionMethod.fromByte(wrapperID)) {
		case RAW:
			break;
		case GZIP:
			wrappedOS = new GZIPOutputStream(os);
			break;

		default:
			throw new RuntimeException("Unknown container.");
		}
		return wrappedOS;
	}

	private byte[] readData(InputStream is, int size, int dataSize, byte method) throws IOException {
		LimitedInputStream limitedInputStream = new LimitedInputStream(size, is);
		InputStream dataIS = wrapInputStream(limitedInputStream, method);
		byte[] unpackedData = new byte[dataSize];
		DataInputStream dis = new DataInputStream(dataIS);
		dis.readFully(unpackedData);
		limitedInputStream.skipToEnd();
		return unpackedData;
	}

	private int writeData(OutputStream os, byte[] data, byte method) throws IOException {
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
		OutputStream wrappedOS = wrapOutputStream(baos, method);
		wrappedOS.write(data);
		wrappedOS.close();
		os.write(baos.getBuffer(), 0, baos.size());

		return baos.size();
	}
}
