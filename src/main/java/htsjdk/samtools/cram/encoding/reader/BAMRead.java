/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
