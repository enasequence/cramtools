package net.sf.cram.digest;

import net.sf.cram.structure.CramRecord;
import net.sf.samtools.SAMRecord;

enum SERIES {
	BASES {
		@Override
		byte[] getBytes(SAMRecord record) {
			return record.getReadBases();
		}

		@Override
		byte[] getBytes(CramRecord record) {
			return record.readBases;
		}
	},
	SCORES {
		@Override
		byte[] getBytes(SAMRecord record) {
			return record.getBaseQualities();
		}

		@Override
		byte[] getBytes(CramRecord record) {
			return record.qualityScores;
		}
	};

	abstract byte[] getBytes(SAMRecord record);

	abstract byte[] getBytes(CramRecord record);

}