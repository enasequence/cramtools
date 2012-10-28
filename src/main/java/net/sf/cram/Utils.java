/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.picard.PicardException;
import net.sf.picard.io.IoUtil;
import net.sf.picard.reference.IndexedFastaSequenceFile;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.sam.SamPairUtil;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTag;
import uk.ac.ebi.ena.sra.cram.io.ExposedByteArrayOutputStream;

public class Utils {
	private static Logger log = Logger.getLogger(Utils.class.getName());

	public final static int toInt(byte[] bytes) {
		return (int) (bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[31] << 24));
	}

	public final static byte[] toBytes(int value) {
		final byte[] bytes = new byte[4];
		bytes[0] = (byte) (value >>> 24);
		bytes[1] = (byte) (value >>> 16);
		bytes[2] = (byte) (value >>> 8);
		bytes[3] = (byte) (value >>> 0);
		return bytes;
	}

	public final static byte[] toBytes(long value) {
		final byte[] bytes = new byte[8];
		for (int i = 0; i < 8; i++)
			bytes[i] = (byte) (value >>> (64 - 8 - i * 8));
		return bytes;
	}

	public static String toBitString(final byte[] b) {
		final char[] bits = new char[8 * b.length];
		for (int i = 0; i < b.length; i++) {
			final byte byteval = b[i];
			int bytei = i << 3;
			int mask = 0x1;
			for (int j = 7; j >= 0; j--) {
				final int bitval = byteval & mask;
				if (bitval == 0) {
					bits[bytei + j] = '0';
				} else {
					bits[bytei + j] = '1';
				}
				mask <<= 1;
			}
		}
		return String.valueOf(bits);
	}

	public static String toBitString(final int value) {
		return toBitString(toBytes(value));
	}

	public static byte[] transformSequence(byte[] bases, boolean compliment, boolean reverse) {
		byte[] result = new byte[bases.length];
		for (int i = 0; i < bases.length; i++) {
			byte base = bases[i];

			int index = reverse ? bases.length - i - 1 : i;

			result[index] = compliment ? complimentBase(base) : base;
		}
		return result;
	}

	public static final byte complimentBase(byte base) {
		switch (base) {
		case 'A':
			return 'T';
		case 'C':
			return 'G';
		case 'G':
			return 'C';
		case 'T':
			return 'A';
		case 'N':
			return 'N';

		default:
			throw new RuntimeException("Unkown base: " + base);
		}
	}

	public static int mostSignificantBit(final long value) {
		int i = 64;
		while (--i >= 0 && (((1L << i) & value)) == 0)
			;
		return i;
	}

	public static Byte[] autobox(byte[] array) {
		Byte[] newArray = new Byte[array.length];
		for (int i = 0; i < array.length; i++)
			newArray[i] = array[i];
		return newArray;
	}

	public static Integer[] autobox(int[] array) {
		Integer[] newArray = new Integer[array.length];
		for (int i = 0; i < array.length; i++)
			newArray[i] = array[i];
		return newArray;
	}

	public static void changeReadLength(SAMRecord record, int newLength) {
		if (newLength == record.getReadLength())
			return;
		if (newLength < 1 || newLength >= record.getReadLength())
			throw new IllegalArgumentException("Cannot change read length to " + newLength);

		List<CigarElement> newCigarElements = new ArrayList<CigarElement>();
		int len = 0;
		for (CigarElement ce : record.getCigar().getCigarElements()) {
			switch (ce.getOperator()) {
			case D:
				break;
			case S:
				// dump = true;
				// len -= ce.getLength();
				// break;
			case M:
			case I:
			case X:
				len += ce.getLength();
				break;

			default:
				throw new IllegalArgumentException("Unexpected cigar operator: " + ce.getOperator() + " in cigar "
						+ record.getCigarString());
			}

			if (len <= newLength) {
				newCigarElements.add(ce);
				continue;
			}
			CigarElement newCe = new CigarElement(ce.getLength() - (record.getReadLength() - newLength),
					ce.getOperator());
			if (newCe.getLength() > 0)
				newCigarElements.add(newCe);
			break;
		}

		byte[] newBases = new byte[newLength];
		System.arraycopy(record.getReadBases(), 0, newBases, 0, newLength);
		record.setReadBases(newBases);

		byte[] newScores = new byte[newLength];
		System.arraycopy(record.getBaseQualities(), 0, newScores, 0, newLength);

		record.setCigar(new Cigar(newCigarElements));
	}

	public static void reversePositionsInRead(CramRecord record) {
		if (record.getReadFeatures() == null || record.getReadFeatures().isEmpty())
			return;
		for (ReadFeature f : record.getReadFeatures())
			f.setPosition((int) (record.getReadLength() - f.getPosition() - 1));

		Collections.reverse(record.getReadFeatures());
	}

	public static byte[] getBasesFromReferenceFile(String referenceFilePath, String seqName, int from, int length) {
		ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(new File(
				referenceFilePath));
		ReferenceSequence sequence = referenceSequenceFile.getSequence(seqName);
		byte[] bases = referenceSequenceFile.getSubsequenceAt(sequence.getName(), from, from + length).getBases();
		return bases;
	}

	public static void capitaliseAndCheckBases(byte[] bases, boolean strict) {
		for (int i = 0; i < bases.length; i++) {
			switch (bases[i]) {
			case 'A':
			case 'C':
			case 'G':
			case 'T':
			case 'N':
				break;
			case 'a':
				bases[i] = 'A';
				break;
			case 'c':
				bases[i] = 'C';
				break;
			case 'g':
				bases[i] = 'G';
				break;
			case 't':
				bases[i] = 'T';
				break;
			case 'n':
				bases[i] = 'N';
				break;

			default:
				if (strict)
					throw new RuntimeException("Illegal base at " + i + ": " + bases[i]);
				else
					bases[i] = 'N';
				break;
			}
		}
	}

	private static int readInt(InputStream in) throws IOException {
		int ch1 = in.read();
		int ch2 = in.read();
		int ch3 = in.read();
		int ch4 = in.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	public static final DataInputStream getNextChunk(DataInputStream dis) throws IOException {
		int compressedBlockSize;
		try {
			compressedBlockSize = readInt(dis);
		} catch (EOFException e) {
			return null;
		}
		byte[] compressedBlockData = new byte[compressedBlockSize];
		try {
			dis.readFully(compressedBlockData);
		} catch (EOFException e) {
			byte[] buf = new byte[6];
			System.arraycopy(compressedBlockData, 0, buf, 0, buf.length);
			System.err.println("Offensive data block start: " + Arrays.toString(buf));
			throw e;
		}

		InputStream gizIS = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(compressedBlockData)));
		ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(8 * 1024);
		IoUtil.copyStream(gizIS, baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.getBuffer());

		CountingInputStream uncompressedCIS = new CountingInputStream(bais);

		// causes problems for random access:
		// GZIPInputStream gizIS = new GZIPInputStream(new
		// ByteArrayInputStream(compressedBlockData));
		// CountingInputStream uncompressedCIS = new CountingInputStream(gizIS);

		DataInputStream uncompressedDIS = new DataInputStream(uncompressedCIS);
		return uncompressedDIS;
	}

	public static SAMFileHeader cramHeader2SamHeader(CramHeader cramHeader) {
		SAMFileHeader samFileHeader = new SAMFileHeader();

		writeComments(samFileHeader, cramHeader.getRecords());
		writeProgramRecords(samFileHeader, cramHeader.getRecords());
		writeSequences(samFileHeader, cramHeader.getRecords());
		writeReadGroups(samFileHeader, cramHeader.getRecords());

		return samFileHeader;
	}

	private static void readProgramRecords(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String tag = "PG";
		for (SAMProgramRecord programRecord : header.getProgramRecords()) {
			CramHeaderRecord headerRecord = new CramHeaderRecord(tag);
			headerRecord.setValueIfNotNull(SAMProgramRecord.PROGRAM_GROUP_ID_TAG, programRecord.getId());

			for (Entry<String, String> entry : programRecord.getAttributes())
				headerRecord.setValue(entry.getKey(), entry.getValue());

			records.add(headerRecord);
		}
	}

	private static void readComments(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String commentTag = "CO";
		for (String comment : header.getComments()) {
			CramHeaderRecord record = new CramHeaderRecord(commentTag);
			record.setValue(commentTag, comment);
			records.add(record);
		}
	}

	private static void readSequences(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String sqTag = "SQ";
		for (SAMSequenceRecord seq : header.getSequenceDictionary().getSequences()) {
			CramHeaderRecord record = new CramHeaderRecord(sqTag);
			record.setValueIfNotNull(SAMSequenceRecord.SEQUENCE_LENGTH_TAG, String.valueOf(seq.getSequenceLength()));
			record.setValueIfNotNull(SAMSequenceRecord.SEQUENCE_NAME_TAG, seq.getSequenceName());

			for (Entry<String, String> entry : seq.getAttributes())
				record.setValue(entry.getKey(), entry.getValue());

			records.add(record);
		}
	}

	private static void readReadGroups(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String sqTag = "RG";
		for (SAMReadGroupRecord rg : header.getReadGroups()) {
			CramHeaderRecord record = new CramHeaderRecord(sqTag);
			record.setValueIfNotNull(SAMReadGroupRecord.READ_GROUP_ID_TAG, rg.getReadGroupId());

			for (Entry<String, String> entry : rg.getAttributes())
				record.setValue(entry.getKey(), entry.getValue());

			records.add(record);

		}
	}

	private static void writeProgramRecords(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String tag = "PG";
		for (CramHeaderRecord record : records) {
			if (!tag.equals(record.getTag()))
				continue;
			String id = record.getValue(SAMProgramRecord.PROGRAM_GROUP_ID_TAG);
			SAMProgramRecord samProgramRecord = new SAMProgramRecord(id);
			for (String key : record.getKeySet())
				if (!SAMProgramRecord.PROGRAM_GROUP_ID_TAG.equals(key))
					samProgramRecord.setAttribute(key, record.getValue(key));

			header.addProgramRecord(samProgramRecord);
		}
	}

	private static void writeComments(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String commentTag = "CO";
		for (CramHeaderRecord record : records)
			if (commentTag.equals(record.getTag()))
				for (String key : record.getKeySet())
					header.addComment(record.getValue(key));
	}

	private static void writeSequences(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String tag = "SQ";
		for (CramHeaderRecord record : records) {
			if (!tag.equals(record.getTag()))
				continue;
			String id = record.getValue(SAMSequenceRecord.SEQUENCE_NAME_TAG);
			int len = Integer.valueOf(record.getValue(SAMSequenceRecord.SEQUENCE_LENGTH_TAG)).intValue();

			SAMSequenceRecord sequenceRecord = new SAMSequenceRecord(id, len);
			for (String key : record.getKeySet())
				if (!SAMSequenceRecord.SEQUENCE_NAME_TAG.equals(key)
						&& !SAMSequenceRecord.SEQUENCE_LENGTH_TAG.equals(key))
					sequenceRecord.setAttribute(key, record.getValue(key));

			header.addSequence(sequenceRecord);
		}
	}

	private static void writeReadGroups(SAMFileHeader header, Collection<CramHeaderRecord> records) {
		String tag = "RG";
		for (CramHeaderRecord record : records) {
			if (!tag.equals(record.getTag()))
				continue;
			String id = record.getValue(SAMReadGroupRecord.READ_GROUP_ID_TAG);
			SAMReadGroupRecord readGroupRecord = new SAMReadGroupRecord(id);
			for (String key : record.getKeySet())
				if (!SAMReadGroupRecord.READ_GROUP_ID_TAG.equals(key))
					readGroupRecord.setAttribute(key, record.getValue(key));

			header.addReadGroup(readGroupRecord);
		}
	}

	public static List<CramHeaderRecord> getCramHeaderRecords(SAMFileHeader samHeader) {
		List<CramHeaderRecord> headerRecords = new ArrayList<CramHeaderRecord>();
		readComments(samHeader, headerRecords);
		readProgramRecords(samHeader, headerRecords);
		readSequences(samHeader, headerRecords);
		readReadGroups(samHeader, headerRecords);

		return headerRecords;
	}

	/**
	 * Reads and discards first bytes which must correspond to the CRAM magic.
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static boolean isCRAM(InputStream is) throws IOException {
		byte[] magick = "CRAM".getBytes();
		for (byte b : magick)
			if (is.read() != b)
				return false;

		return true;
	}

	private static int getMajorVersion(String version) {
		if (version.contains("-"))
			version = version.split("-")[0];
		int majorVersion = (int) ((Float.valueOf(version)) * 10);
		return majorVersion;
	}

	public static boolean isCompatible(String version) {
		String currentVersion = CramHeader.VERSION;
		int currentMajorVersion = getMajorVersion(currentVersion);
		int majorVersion = getMajorVersion(version);

		if (currentMajorVersion == majorVersion)
			return true;

		if (majorVersion == 7 && currentMajorVersion == 8)
			return true;

		return false;
	}

	/**
	 * Copied from net.sf.picard.sam.SamPairUtil. This is a more permissive
	 * version of the method, which does not reset alignment start and reference
	 * for unmapped reads.
	 * 
	 * @param rec1
	 * @param rec2
	 * @param header
	 */
	public static void setLooseMateInfo(final SAMRecord rec1, final SAMRecord rec2, final SAMFileHeader header) {
		if (rec1.getReferenceName() != SAMRecord.NO_ALIGNMENT_REFERENCE_NAME
				&& rec1.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
			rec1.setReferenceIndex(header.getSequenceIndex(rec1.getReferenceName()));
		if (rec2.getReferenceName() != SAMRecord.NO_ALIGNMENT_REFERENCE_NAME
				&& rec2.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
			rec2.setReferenceIndex(header.getSequenceIndex(rec2.getReferenceName()));

		// If neither read is unmapped just set their mate info
		if (!rec1.getReadUnmappedFlag() && !rec2.getReadUnmappedFlag()) {

			rec1.setMateReferenceIndex(rec2.getReferenceIndex());
			rec1.setMateAlignmentStart(rec2.getAlignmentStart());
			rec1.setMateNegativeStrandFlag(rec2.getReadNegativeStrandFlag());
			rec1.setMateUnmappedFlag(false);
			rec1.setAttribute(SAMTag.MQ.name(), rec2.getMappingQuality());

			rec2.setMateReferenceIndex(rec1.getReferenceIndex());
			rec2.setMateAlignmentStart(rec1.getAlignmentStart());
			rec2.setMateNegativeStrandFlag(rec1.getReadNegativeStrandFlag());
			rec2.setMateUnmappedFlag(false);
			rec2.setAttribute(SAMTag.MQ.name(), rec1.getMappingQuality());
		}
		// Else if they're both unmapped set that straight
		else if (rec1.getReadUnmappedFlag() && rec2.getReadUnmappedFlag()) {
			rec1.setMateNegativeStrandFlag(rec2.getReadNegativeStrandFlag());
			rec1.setMateUnmappedFlag(true);
			rec1.setAttribute(SAMTag.MQ.name(), null);
			rec1.setInferredInsertSize(0);

			rec2.setMateNegativeStrandFlag(rec1.getReadNegativeStrandFlag());
			rec2.setMateUnmappedFlag(true);
			rec2.setAttribute(SAMTag.MQ.name(), null);
			rec2.setInferredInsertSize(0);
		}
		// And if only one is mapped copy it's coordinate information to the
		// mate
		else {
			final SAMRecord mapped = rec1.getReadUnmappedFlag() ? rec2 : rec1;
			final SAMRecord unmapped = rec1.getReadUnmappedFlag() ? rec1 : rec2;

			mapped.setMateReferenceIndex(unmapped.getReferenceIndex());
			mapped.setMateAlignmentStart(unmapped.getAlignmentStart());
			mapped.setMateNegativeStrandFlag(unmapped.getReadNegativeStrandFlag());
			mapped.setMateUnmappedFlag(true);
			mapped.setInferredInsertSize(0);

			unmapped.setMateReferenceIndex(mapped.getReferenceIndex());
			unmapped.setMateAlignmentStart(mapped.getAlignmentStart());
			unmapped.setMateNegativeStrandFlag(mapped.getReadNegativeStrandFlag());
			unmapped.setMateUnmappedFlag(false);
			unmapped.setInferredInsertSize(0);
		}

		boolean firstIsFirst = rec1.getAlignmentStart() < rec2.getAlignmentStart();
		int insertSize = firstIsFirst ? SamPairUtil.computeInsertSize(rec1, rec2) : SamPairUtil.computeInsertSize(rec2,
				rec1);

		rec1.setInferredInsertSize(firstIsFirst ? insertSize : -insertSize);
		rec2.setInferredInsertSize(firstIsFirst ? -insertSize : insertSize);

	}

	public static int computeInsertSize(SAMRecord firstEnd, SAMRecord secondEnd) {
		if (firstEnd.getAlignmentStart() < secondEnd.getAlignmentStart())
			return SamPairUtil.computeInsertSize(firstEnd, secondEnd);
		else
			return SamPairUtil.computeInsertSize(secondEnd, firstEnd);
	}

	public static IndexedFastaSequenceFile createIndexedFastaSequenceFile(File file) throws CramException,
			FileNotFoundException {
		if (IndexedFastaSequenceFile.canCreateIndexedFastaReader(file)) {
			IndexedFastaSequenceFile ifsFile = new IndexedFastaSequenceFile(file);

			return ifsFile;
		} else
			throw new CramException(
					"Reference fasta file is not indexed or index file not found. Try executing 'samtools faidx "
							+ file.getAbsolutePath() + "'");
	}

	public static ReferenceSequence getReferenceSequenceOrNull(ReferenceSequenceFile rsFile, String name) {
		ReferenceSequence rs = null;
		try {
			return rsFile.getSequence(name);
		} catch (PicardException e) {
			return null;
		}
	}

	private static final Pattern chrPattern = Pattern.compile("chr.*", Pattern.CASE_INSENSITIVE);

	public static byte[] getBasesOrNull(ReferenceSequenceFile rsFile, String name, int start, int len) {
		ReferenceSequence rs = getReferenceSequenceOrNull(rsFile, name);
		if (rs == null && name.equals("M")) {
			rs = getReferenceSequenceOrNull(rsFile, "MT");
		}

		if (rs == null && name.equals("MT")) {
			rs = getReferenceSequenceOrNull(rsFile, "M");
		}

		boolean chrPatternMatch = chrPattern.matcher(name).matches();
		if (rs == null) {
			if (chrPatternMatch)
				rs = getReferenceSequenceOrNull(rsFile, name.substring(3));
			else
				rs = getReferenceSequenceOrNull(rsFile, "chr" + name);
		}
		if (rs == null)
			return null;

		if (len < 1)
			return rs.getBases();
		else
			return rsFile.getSubsequenceAt(rs.getName(), 1, len).getBases();
	}

	public static byte[] getReferenceSequenceBases(ReferenceSequenceFile referenceSequenceFile, String seqName)
			throws CramException {
		long time1 = System.currentTimeMillis();
		byte[] refBases = Utils.getBasesOrNull(referenceSequenceFile, seqName, 1, 0);
		if (refBases == null)
			throw new CramException("Reference sequence " + seqName + " not found in the fasta file "
					+ referenceSequenceFile.toString());

		long time2 = System.currentTimeMillis();
		log.debug(String.format("Reference sequence %s read in %.2f seconds.", seqName, (time2 - time1) / 1000f));

		Utils.capitaliseAndCheckBases(refBases, false);

		long time3 = System.currentTimeMillis();
		log.debug(String.format("Reference sequence normalized in %.2f seconds.", (time3 - time2) / 1000f));
		return refBases;
	}

	/**
	 * A rip off samtools bam_md.c
	 * 
	 * @param record
	 * @param ref
	 * @param flag
	 * @return
	 */
	public static void calculateMdAndNmTags(SAMRecord record, byte[] ref, boolean calcMD, boolean calcNM) {
		Cigar cigar = record.getCigar();
		List<CigarElement> cigarElements = cigar.getCigarElements();
		byte[] seq = record.getReadBases();
		int start = record.getAlignmentStart() - 1;
		int i, x, y, u = 0;
		int nm = 0;
		StringBuffer str = new StringBuffer();

		for (i = y = 0, x = start; i < cigarElements.size(); ++i) {
			CigarElement ce = cigarElements.get(i);
			int j, l = ce.getLength();
			CigarOperator op = ce.getOperator();
			if (op == CigarOperator.MATCH_OR_MISMATCH || op == CigarOperator.EQ || op == CigarOperator.X) {
				for (j = 0; j < l; ++j) {
					int z = y + j;

					if (ref.length <= x + j)
						break; // out of boundary

					int c1 = 0;
					int c2 = 0;
					// try {
					c1 = seq[z];
					c2 = ref[x + j];
					// } catch (ArrayIndexOutOfBoundsException e) {
					// System.err.println("Offending record: ");
					// System.err.println(record.getSAMString());
					// System.err.printf("z=%d; x=%d; j=%d; i=%d; y=%d, l=%d\n",
					// z, x, j, i, y, l);
					// System.err.printf("Cigar op=%s\n", op.name());
					// throw e ;
					// }

					if ((c1 == c2 && c1 != 15 && c2 != 15) || c1 == 0) {
						// a match
						++u;
					} else {
						str.append(u);
						str.appendCodePoint(ref[x + j]);
						u = 0;
						++nm;
					}
				}
				if (j < l)
					break;
				x += l;
				y += l;
			} else if (op == CigarOperator.DELETION) {
				str.append(u);
				str.append('^');
				for (j = 0; j < l; ++j) {
					if (ref[x + j] == 0)
						break;
					str.appendCodePoint(ref[x + j]);
				}
				u = 0;
				if (j < l)
					break;
				x += l;
				nm += l;
			} else if (op == CigarOperator.INSERTION || op == CigarOperator.SOFT_CLIP) {
				y += l;
				if (op == CigarOperator.INSERTION)
					nm += l;
			} else if (op == CigarOperator.SKIPPED_REGION) {
				x += l;
			}
		}
		str.append(u);

		if (calcMD)
			record.setAttribute(SAMTag.MD.name(), str.toString());
		if (calcNM)
			record.setAttribute(SAMTag.NM.name(), nm);
	}

	public static int[][] sortByFirst(int[] array1, int[] array2) {
		int[][] sorted = new int[array1.length][2];
		for (int i = 0; i < array1.length; i++) {
			sorted[i][0] = array1[i];
			sorted[i][1] = array2[i];
		}

		Arrays.sort(sorted, intArray_2_Comparator);

		int[][] result = new int[2][array1.length];
		for (int i = 0; i < array1.length; i++) {
			result[0][i] = sorted[i][0];
			result[1][i] = sorted[i][1];
		}

		return result;
	}

	private static Comparator<int[]> intArray_2_Comparator = new Comparator<int[]>() {

		@Override
		public int compare(int[] o1, int[] o2) {
			int result = o1[0] - o2[0];
			if (result != 0)
				return -result;

			return -(o1[1] - o2[1]);
		}
	};
}
