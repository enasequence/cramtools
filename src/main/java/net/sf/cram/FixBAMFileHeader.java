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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import net.sf.cram.common.Utils;
import net.sf.cram.ref.ReferenceSource;

public class FixBAMFileHeader {
	private static Log log = Log.getInstance(FixBAMFileHeader.class);
	private boolean confirmMD5 = false;
	private String sequenceUrlPattern = "http://www.ebi.ac.uk/ena/cram/md5/%s";
	private boolean injectURI = false;
	private boolean ignoreMD5Mismatch = false;

	private ReferenceSource referenceSource;

	public FixBAMFileHeader(ReferenceSource referenceSource) {
		this.referenceSource = referenceSource;
	}

	public void fixSequences(List<SAMSequenceRecord> sequenceRecords) throws MD5MismatchError {
		for (SAMSequenceRecord sequenceRecord : sequenceRecords)
			fixSequence(sequenceRecord);
	}

	public void fixSequence(SAMSequenceRecord sequenceRecord) throws MD5MismatchError {
		String found_md5 = sequenceRecord.getAttribute(SAMSequenceRecord.MD5_TAG);
		if (confirmMD5) {
			if (found_md5 != null) {
				byte[] bytes = referenceSource.getReferenceBases(sequenceRecord, true);
				if (bytes == null) {
					String message = String.format("Reference bases not found: name %s, length %d, md5 %s.",
							sequenceRecord.getSequenceName(), sequenceRecord.getSequenceLength(), found_md5);
					log.error(message);
					throw new RuntimeException(message);
				}
				log.info("Confirming reference sequence md5: " + sequenceRecord.getSequenceName());
				String md5 = Utils.calculateMD5String(bytes);
				if (!md5.equals(found_md5)) {
					if (ignoreMD5Mismatch) {
						log.warn(String.format(
								"Sequence id=%d, len=%d, name=%s has incorrect md5=%s. Replaced with %s.",
								sequenceRecord.getSequenceIndex(), sequenceRecord.getSequenceLength(),
								sequenceRecord.getSequenceName(), found_md5, md5));
						sequenceRecord.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
					} else
						throw new MD5MismatchError(sequenceRecord, md5);
				}
				if (sequenceRecord.getSequenceLength() != bytes.length) {
					log.warn(String.format("Sequence id=%d, name=%s has incorrect length=%d. Replaced with %d.",
							sequenceRecord.getSequenceIndex(), sequenceRecord.getSequenceName(),
							sequenceRecord.getSequenceLength(), bytes.length));
					sequenceRecord.setSequenceLength(bytes.length);
				}
			} else {
				log.info("Reference sequence MD5 not found, calculating: " + sequenceRecord.getSequenceName());
				byte[] bytes = referenceSource.getReferenceBases(sequenceRecord, true);
				if (bytes == null) {
					String message = String.format("Reference bases not found: name %s, length %d.",
							sequenceRecord.getSequenceName(), sequenceRecord.getSequenceLength());
					log.error(message);
					throw new RuntimeException(message);
				}
				String md5 = Utils.calculateMD5String(bytes);
				sequenceRecord.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
			}
		}

		if (injectURI) {
			sequenceRecord.setAttribute(SAMSequenceRecord.URI_TAG,
					String.format(sequenceUrlPattern, sequenceRecord.getAttribute(SAMSequenceRecord.MD5_TAG)));
		}
	}

	public void addPG(SAMFileHeader header, String program, String cmd, String version) {
		SAMProgramRecord programRecord = header.createProgramRecord();
		programRecord.setCommandLine(cmd);
		programRecord.setProgramName(program);
		programRecord.setProgramVersion(version);
	}

	public void addCramtoolsPG(SAMFileHeader header) {
		String cmd = "java " + Utils.getJavaCommand();
		String version = Utils.CRAM_VERSION.toString();

		addPG(header, "cramtools", cmd, version);
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

	public boolean fixHeaderInFile(File cramFile) throws IOException, MD5MismatchError {
		FileInputStream fis = new FileInputStream(cramFile);
		CramHeader cramHeader = CramIO.readCramHeader(fis);

		fixSequences(cramHeader.getSamFileHeader().getSequenceDictionary().getSequences());
		String cmd = "fixheader";
		String version = getClass().getPackage().getImplementationVersion();
		addPG(cramHeader.getSamFileHeader(), "cramtools", cmd, version);

		CramHeader newHeader = cramHeader.clone();

		return CramIO.replaceCramHeader(cramFile, newHeader);
	}

	public static class MD5MismatchError extends RuntimeException {

		private SAMSequenceRecord sequenceRecord;
		private String actualMD5;

		public MD5MismatchError(SAMSequenceRecord sequenceRecord, String md5) {
			super(String.format("MD5 mismatch for sequence %s:%s", sequenceRecord.getSequenceName(), md5));
			this.sequenceRecord = sequenceRecord;
			this.actualMD5 = md5;
		}

		public String getActualMD5() {
			return actualMD5;
		}

		public void setActualMD5(String actualMD5) {
			this.actualMD5 = actualMD5;
		}

		public SAMSequenceRecord getSequenceRecord() {
			return sequenceRecord;
		}

		public void setSequenceRecord(SAMSequenceRecord sequenceRecord) {
			this.sequenceRecord = sequenceRecord;
		}

	}

	public boolean isIgnoreMD5Mismatch() {
		return ignoreMD5Mismatch;
	}

	public void setIgnoreMD5Mismatch(boolean ignoreMD5Mismatch) {
		this.ignoreMD5Mismatch = ignoreMD5Mismatch;
	}

}
