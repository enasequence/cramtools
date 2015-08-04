package htsjdk.samtools.cram.encoding.reader;

import java.nio.ByteBuffer;
import java.util.Arrays;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.encoding.reader.NameCollate.IRead;

public class FastqRead implements IRead {
	int templateIndex;
	long generation;
	byte[] data;
	int nameLen;
	int nameBaseLen;
	FastqRead next;
	long age = 0;

	public FastqRead(int readLength, byte[] name, boolean appendSegmentIndex, int templateIndex, byte[] bases,
			byte[] scores) {
		ByteBuffer buf = null;
		nameBaseLen = name.length;

		if (appendSegmentIndex) {
			this.nameLen = name.length + (templateIndex > 0 ? 2 : 0);
			this.templateIndex = templateIndex;

			data = new byte[nameLen + 2 * readLength + 6];

			buf = ByteBuffer.wrap(data);
			buf.put((byte) '@');
			buf.put(name);
			if (templateIndex > 0) {
				buf.put((byte) '/');
				buf.put((byte) (48 + templateIndex));
			}
		} else {
			this.nameLen = name.length;
			this.templateIndex = templateIndex;

			data = new byte[nameLen + 2 * readLength + 6];

			buf = ByteBuffer.wrap(data);
			buf.put((byte) '@');
			buf.put(name);
		}
		buf.put((byte) '\n');

		buf.put(bases, 0, readLength);
		buf.put((byte) '\n');

		buf.put("+\n".getBytes());

		if (scores != null)
			buf.put(scores, 0, readLength);
		else
			for (int i = 0; i < readLength; i++)
				buf.put((byte) 33);

		buf.put((byte) '\n');
	}

	@Override
	public int compareTo(IRead read) {
		if (!(read instanceof FastqRead))
			return -1;

		FastqRead r = (FastqRead) read;

		int result = nameBaseLen - r.nameBaseLen;
		if (result != 0)
			return result;

		for (int i = nameBaseLen - 1; i > -1; i--) {
			result = data[1 + i] - r.data[1 + i];
			if (result != 0)
				return result;
		}

		return 0;
	}

	@Override
	public long getAge() {
		return age;
	}

	@Override
	public void setAge(long age) {
		this.age = age;
	}

	public SAMRecord toSAMRecord(SAMFileHeader header) {
		SAMRecord record = new SAMRecord(header);
		String name = null;
		if (data[nameLen - 1] == '/' && Character.isDigit(data[nameLen]))
			name = new String(data, 1, nameLen - 2);
		else
			name = new String(data, 1, nameLen - 2);
		record.setReadName(name);
		int readLen = (data.length - this.nameLen - 4 - 1) / 2;
		byte[] bases = Arrays.copyOfRange(data, nameLen + 2, nameLen + 2 + readLen);
		record.setReadBases(bases);

		byte[] scores = Arrays.copyOfRange(data, nameLen + 3 + 1 + readLen + 1, nameLen + 3 + 1 + 2 * readLen + 1);
		record.setBaseQualityString(new String(scores));

		record.setReadUnmappedFlag(true);
		switch (templateIndex) {
		case 0:
			record.setReadPairedFlag(false);
			break;
		case 1:
			record.setReadPairedFlag(true);
			record.setFirstOfPairFlag(true);
			break;
		case 2:
			record.setReadPairedFlag(true);
			record.setSecondOfPairFlag(true);
			break;

		default:
			break;
		}
		return record;
	}
}