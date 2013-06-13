/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram;

import java.util.List;

import net.sf.cram.common.Utils;
import net.sf.cram.ref.ReferenceSource;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMSequenceRecord;

public class FixBAMFileHeader {
	private static Log log = Log.getInstance(FixBAMFileHeader.class);
	private boolean confirmMD5 = false;
	private String sequenceUrlPattern = "http://www.ebi.ac.uk/ena/cram/md5/%s";
	private boolean injectURI = false ;

	private ReferenceSource referenceSource;

	public FixBAMFileHeader(ReferenceSource referenceSource) {
		this.referenceSource = referenceSource;
	}

	public void fixSequences(List<SAMSequenceRecord> sequenceRecords) {
		for (SAMSequenceRecord sequenceRecord : sequenceRecords)
			fixSequence(sequenceRecord);
	}

	public void fixSequence(SAMSequenceRecord sequenceRecord) {
		String found_md5 = sequenceRecord
				.getAttribute(SAMSequenceRecord.MD5_TAG);
		if (found_md5 != null) {
			if (!confirmMD5) {
				byte[] bytes = referenceSource.getReferenceBases(
						sequenceRecord, true);
				String md5 = Utils.calculateMD5_RTE(bytes);
				if (!md5.equals(found_md5)) {
					log.warn(String
							.format("Sequence id=%d, len=%d, name=%s has incorrect md5=%s. Replaced with %s.",
									sequenceRecord.getSequenceIndex(),
									sequenceRecord.getSequenceLength(),
									sequenceRecord.getSequenceName(),
									found_md5, md5));
					sequenceRecord.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
				}
				if (sequenceRecord.getSequenceLength() != bytes.length) {
					log.warn(String
							.format("Sequence id=%d, name=%s has incorrect length=%d. Replaced with %d.",
									sequenceRecord.getSequenceIndex(),
									sequenceRecord.getSequenceName(),
									sequenceRecord.getSequenceLength(),
									bytes.length));
					sequenceRecord.setSequenceLength(bytes.length);
				}
			}
		} else {
			byte[] bytes = referenceSource.getReferenceBases(sequenceRecord,
					true);
			String md5 = Utils.calculateMD5_RTE(bytes);
			sequenceRecord.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
		}

		if (injectURI) {
			sequenceRecord.setAttribute(SAMSequenceRecord.URI_TAG, String
					.format(sequenceUrlPattern, sequenceRecord
							.getAttribute(SAMSequenceRecord.MD5_TAG)));
		}
	}

	public void addPG(SAMFileHeader header, String program, String cmd,
			String version) {
		SAMProgramRecord programRecord = header.createProgramRecord();
		programRecord.setCommandLine(cmd);
		programRecord.setProgramName(program);
		programRecord.setProgramName(version);
	}

	public boolean isConfirmMD5() {
		return confirmMD5;
	}

	public void setConfirmMD5(boolean confirmMD5) {
		this.confirmMD5 = confirmMD5;
	}

	public String getSequenceUrlPattern() {
		return sequenceUrlPattern;
	}

	public void setSequenceUrlPattern(String sequenceUrlPattern) {
		this.sequenceUrlPattern = sequenceUrlPattern;
	}

	public ReferenceSource getReferenceSource() {
		return referenceSource;
	}
	
	public boolean isInjectURI() {
		return injectURI;
	}

	public void setInjectURI(boolean injectURI) {
		this.injectURI = injectURI;
	}
}
