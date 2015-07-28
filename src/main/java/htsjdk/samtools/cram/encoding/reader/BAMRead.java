package htsjdk.samtools.cram.encoding.reader;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.encoding.reader.NameCollate.IRead;

public class BAMRead implements IRead {
	private SAMRecord record;
	private long age = 0;

	public BAMRead(SAMRecord record) {
		super();
		this.record = record;
	}

	@Override
	public long getAge() {
		return age;
	}

	@Override
	public void setAge(long age) {
		this.age = age;
	}

	public SAMRecord getRecord() {
		return record;
	};

	@Override
	public int compareTo(IRead o) {
		SAMRecord foe = ((BAMRead) o).getRecord();
		if (record.getReadName().length() != foe.getReadName().length())
			return record.getReadName().length() - foe.getReadName().length();

		for (int i = record.getReadName().length() - 1; i >= 0; i--) {
			if (record.getReadName().charAt(i) != foe.getReadName().charAt(i))
				return record.getReadName().charAt(i) - foe.getReadName().charAt(i);
		}

		return 0;
	}

}
