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
package net.sf.cram.select;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.lossy.BaseCategory;
import htsjdk.samtools.cram.lossy.Binning;
import htsjdk.samtools.cram.lossy.PreservationPolicy;
import htsjdk.samtools.cram.lossy.QualityScorePreservation;
import htsjdk.samtools.cram.lossy.QualityScoreTreatment;
import htsjdk.samtools.cram.lossy.QualityScoreTreatmentType;
import htsjdk.samtools.cram.lossy.ReadCategory;
import htsjdk.samtools.util.Log;
import net.sf.cram.AlignmentSliceQuery;
import net.sf.cram.Bam2Cram;
import net.sf.cram.CramTools.LevelConverter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import net.sf.cram.ref.ReferenceSource;

public class SamRecordComparision {
	private static final byte DEFAULT_SCORE = 30;
	private int maxValueLen = 15;
	private static Log log = Log.getInstance(SamRecordComparision.class);

	public EnumSet<FIELD_TYPE> fields = EnumSet.allOf(FIELD_TYPE.class);
	public Set<String> tagsToIgnore = new TreeSet<String>();
	public Set<String> tagsToCompare = new TreeSet<String>();
	public boolean compareTags = false;
	public int ignoreFlags = 0;
	public int ignoreTLENDiff = 0;

	public static class SamRecordDiscrepancy {
		public FIELD_TYPE field;
		public String tagId;
		public long recordCounter;

		public SAMRecord record1, record2;

		public int prematureEnd = 0;

	}

	public static Object getValue(SAMRecord record, FIELD_TYPE field, String tagId) {
		if (field == null)
			throw new IllegalArgumentException("Record field is null.");

		switch (field) {
		case QNAME:
			return record.getReadName();
		case FLAG:
			return Integer.toString(record.getFlags());
		case RNAME:
			return record.getReferenceName();
		case POS:
			return Integer.toString(record.getAlignmentStart());
		case MAPQ:
			return Integer.toString(record.getMappingQuality());
		case CIGAR:
			return record.getCigarString();
		case RNEXT:
			return record.getMateReferenceName();
		case PNEXT:
			return Integer.toString(record.getMateAlignmentStart());
		case TLEN:
			return Integer.toString(record.getInferredInsertSize());
		case SEQ:
			return record.getReadString();
		case QUAL:
			return record.getBaseQualityString();

		case TAG:
			if (tagId == null)
				throw new IllegalArgumentException("Tag mismatch reqiues tag id. ");
			return record.getAttribute(tagId);

		default:
			throw new IllegalArgumentException("Unknown record field: " + field.name());
		}
	}

	public boolean compareFieldValue(SAMRecord r1, SAMRecord r2, FIELD_TYPE field, String tagId) {
		if (field == null)
			throw new IllegalArgumentException("Record field is null.");

		if (field == FIELD_TYPE.FLAG) {
			int f1 = r1.getFlags() & ~ignoreFlags;
			int f2 = r2.getFlags() & ~ignoreFlags;

			return f1 == f2;
		}

		if (field == FIELD_TYPE.TLEN) {
			int t1 = r1.getInferredInsertSize();
			int t2 = r2.getInferredInsertSize();

			return Math.abs(t1 - t2) <= ignoreTLENDiff;
		}

		Object value1 = getValue(r1, field, tagId);
		Object value2 = getValue(r2, field, tagId);
		return compareObjects(value1, value2);
	}

	private static boolean compareObjects(Object o1, Object o2) {
		if (o1 == null && o2 == null)
			return true;
		if (o1 == null || o2 == null)
			return false;

		if (o1.equals(o2))
			return true;

		if (o1.getClass().isArray() && o2.getClass().isArray()) {
			if (o1 instanceof byte[] && o2 instanceof byte[])
				return Arrays.equals((byte[]) o1, (byte[]) o2);

			if (o1 instanceof short[] && o2 instanceof short[])
				return Arrays.equals((short[]) o1, (short[]) o2);

			return Arrays.equals((Object[]) o1, (Object[]) o2);
		}

		if (o1 instanceof SAMRecord.SAMTagAndValue && o2 instanceof SAMRecord.SAMTagAndValue) {
			SAMRecord.SAMTagAndValue t1 = (SAMRecord.SAMTagAndValue) o1;
			SAMRecord.SAMTagAndValue t2 = (SAMRecord.SAMTagAndValue) o2;

			return t1.tag.equals(t2.tag) && compareObjects(t1.value, t2.value);
		}

		return false;
	}

	private boolean compareTags(SAMRecord r1, SAMRecord r2, long recordCounter, List<SamRecordDiscrepancy> list) {
		if (!compareTags)
			return true;

		Map<String, SAMRecord.SAMTagAndValue> m1 = new TreeMap<String, SAMRecord.SAMTagAndValue>();
		for (SAMRecord.SAMTagAndValue t : r1.getAttributes())
			m1.put(t.tag, t);

		Map<String, SAMRecord.SAMTagAndValue> m2 = new TreeMap<String, SAMRecord.SAMTagAndValue>();
		for (SAMRecord.SAMTagAndValue t : r2.getAttributes())
			m2.put(t.tag, t);

		boolean equal = true;
		for (String id : m1.keySet()) {
			if (tagsToIgnore.contains(id))
				continue;
			if (!tagsToCompare.isEmpty() && !tagsToCompare.contains(id))
				continue;

			if (m2.containsKey(id) && compareObjects(m1.get(id), m2.get(id)))
				continue;

			SamRecordDiscrepancy d = new SamRecordDiscrepancy();
			d.record1 = r1;
			d.record2 = r2;
			d.field = FIELD_TYPE.TAG;
			d.tagId = id;
			d.recordCounter = recordCounter;
			list.add(d);

			equal = false;
		}

		for (String id : m2.keySet()) {
			if (tagsToIgnore.contains(id))
				continue;
			if (!tagsToCompare.isEmpty() && !tagsToCompare.contains(id))
				continue;

			if (m1.containsKey(id) && compareObjects(m1.get(id), m2.get(id)))
				continue;

			SamRecordDiscrepancy d = new SamRecordDiscrepancy();
			d.record1 = r1;
			d.record2 = r2;
			d.field = FIELD_TYPE.TAG;
			d.tagId = id;
			d.recordCounter = recordCounter;
			list.add(d);

			equal = false;
		}

		return equal;
	}

	public void compareRecords(SAMRecord r1, SAMRecord r2, long recordCounter, List<SamRecordDiscrepancy> list) {
		// if (!r1.getReadName().equals(r2.getReadName())
		// || r1.getAlignmentStart() != r2.getAlignmentStart()) {
		// System.err.println("Name mismatch: ");
		// System.err.println("\t"+r1.getSAMString());
		// System.err.println("\t"+r2.getSAMString());
		// }
		for (FIELD_TYPE field : fields) {
			String tagId = null;
			if (field == FIELD_TYPE.TAG) {
				compareTags(r1, r2, recordCounter, list);
			} else {
				if (!compareFieldValue(r1, r2, field, tagId)) {
					SamRecordDiscrepancy d = new SamRecordDiscrepancy();
					d.record1 = r1;
					d.record2 = r2;
					d.field = field;
					d.tagId = null;
					d.recordCounter = recordCounter;
					list.add(d);
				}
			}
		}

	}

	public List<SamRecordDiscrepancy> compareRecords(SAMRecordIterator it1, SAMRecordIterator it2, int maxDiscrepandcies) {
		List<SamRecordDiscrepancy> discrepancies = new ArrayList<SamRecordComparision.SamRecordDiscrepancy>();
		long recordCounter = 0;

		while (it1.hasNext() && it2.hasNext() && discrepancies.size() < maxDiscrepandcies) {
			recordCounter++;
			SAMRecord record1 = it1.next();
			SAMRecord record2 = it2.next();

			compareRecords(record1, record2, recordCounter, discrepancies);
		}

		if (it1.hasNext() && !it2.hasNext()) {
			SamRecordDiscrepancy d = new SamRecordDiscrepancy();
			d.record1 = it1.next();
			d.prematureEnd = 2;
			discrepancies.add(d);
		} else if (it2.hasNext() && !it1.hasNext()) {
			SamRecordDiscrepancy d = new SamRecordDiscrepancy();
			d.record2 = it2.next();
			d.prematureEnd = 1;
			discrepancies.add(d);
		}

		return discrepancies;
	}

	private static String print(String value, int maxLen) {
		if (value == null)
			return "!NULL!";
		if (value.length() <= maxLen)
			return value;
		else
			return value.substring(0, Math.min(maxLen, value.length())) + "...";
	}

	private static void createDiscrepancyTable(String tableName, Connection c) throws SQLException {
		System.out.println(tableName);
		PreparedStatement ps = c
				.prepareStatement("CREATE TABLE IF NOT EXISTS "
						+ tableName
						+ "(counter INT, field VARCHAR, tag VARCHAR, premature int, value1 VARCHAR, value2 VARCHAR, name1 VARCHAR, name2 VARCHAR, record1 VARCHAR, record2 VARCHAR);");
		ps.executeUpdate();
		c.commit();
	}

	private static void dbLog(String tableName, Iterator<SamRecordDiscrepancy> it, Connection c) throws SQLException {
		PreparedStatement ps = c
				.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		while (it.hasNext()) {
			SamRecordDiscrepancy d = it.next();
			int column = 1;
			ps.setLong(column++, d.recordCounter);
			ps.setString(column++, d.field.name());
			ps.setString(column++, d.tagId);
			ps.setInt(column++, d.prematureEnd);

			String value1 = null;
			String value2 = null;
			switch (d.prematureEnd) {
			case 0:
				value1 = SAMRecordField.toString(getValue(d.record1, d.field, d.tagId));
				ps.setString(column++, value1);

				value2 = SAMRecordField.toString(getValue(d.record2, d.field, d.tagId));
				ps.setString(column++, value2);

				ps.setString(column++, d.record1.getReadName());
				ps.setString(column++, d.record2.getReadName());

				ps.setString(column++, d.record1.getSAMString());
				ps.setString(column++, d.record2.getSAMString());
				break;
			case 1:
				ps.setString(column++, null);

				value2 = SAMRecordField.toString(getValue(d.record2, FIELD_TYPE.TAG, d.tagId));
				ps.setString(column++, value2);

				ps.setString(column++, null);
				ps.setString(column++, d.record2.getReadName());

				ps.setString(column++, null);
				ps.setString(column++, d.record2.getSAMString());
				break;
			case 2:
				value1 = SAMRecordField.toString(getValue(d.record1, FIELD_TYPE.TAG, d.tagId));
				ps.setString(column++, value1);

				ps.setString(column++, null);

				ps.setString(column++, d.record1.getReadName());
				ps.setString(column++, null);

				ps.setString(column++, d.record1.getSAMString());
				ps.setString(column++, null);
				break;

			default:
				break;
			}

			ps.addBatch();
		}
		ps.executeBatch();
		c.commit();
	}

	public void log(SamRecordDiscrepancy d, boolean dumpRecords, PrintStream ps) {
		switch (d.prematureEnd) {
		case 0:
			if (d.field != FIELD_TYPE.TAG) {
				String value1 = SAMRecordField.toString(getValue(d.record1, d.field, null));
				String value2 = SAMRecordField.toString(getValue(d.record2, d.field, null));
				ps.println(String.format("FIELD:\t%d\t%s\t%s\t%s", d.recordCounter, d.field.name(),
						print(value1, maxValueLen), print(value2, maxValueLen)));
			} else {
				String value1 = SAMRecordField.toString(getValue(d.record1, FIELD_TYPE.TAG, d.tagId));
				String value2 = SAMRecordField.toString(getValue(d.record2, FIELD_TYPE.TAG, d.tagId));
				ps.println(String.format("TAG:\t%d\t%s\t%s\t%s\t%s", d.recordCounter, d.field.name(), d.tagId,
						print(value1, maxValueLen), print(value2, maxValueLen)));
			}
			if (dumpRecords) {
				ps.print("\t" + d.record1.getSAMString());
				ps.print("\t" + d.record2.getSAMString());
			}
			break;
		case 1:
			ps.println(String.format("PREMATURE:\t%d\t%d", d.recordCounter, d.prematureEnd));
			if (dumpRecords)
				ps.print("\t" + d.record2.getSAMString());
			break;
		case 2:
			ps.println(String.format("PREMATURE:\t%d\t%d", d.recordCounter, d.prematureEnd));
			if (dumpRecords)
				ps.print("\t" + d.record1.getSAMString());
			break;

		default:
			throw new IllegalArgumentException("Unknown premature end value: " + d.prematureEnd);
		}

	}

	private static class MutableInt {
		int value;

		public MutableInt(int value) {
			this.value = value;
		}
	}

	public void summary(List<SamRecordDiscrepancy> list, PrintStream ps) {
		Map<String, MutableInt> map = new HashMap<String, SamRecordComparision.MutableInt>();
		int prematureEndOfFile = -1;
		for (SamRecordDiscrepancy d : list) {
			if (d.field == null) {
				prematureEndOfFile = d.prematureEnd;
				continue;
			}

			String id = d.field == FIELD_TYPE.TAG ? d.tagId : d.field.name();
			MutableInt m = map.get(id);
			if (m == null) {
				m = new MutableInt(0);
				map.put(id, m);
			}
			m.value++;
		}

		if (prematureEndOfFile > -1)
			ps.printf("premature end of file: %d\n", prematureEndOfFile);
		for (FIELD_TYPE f : FIELD_TYPE.values()) {
			if (f == FIELD_TYPE.TAG)
				continue;
			MutableInt m = map.remove(f.name());
			if (m == null)
				continue;
			ps.printf("%s: %d\n", f.name(), m.value);
		}

		for (String id : map.keySet()) {
			MutableInt m = map.get(id);
			ps.printf("%s: %d\n", id, m.value);
		}
	}

	/**
	 * Counts mismatches in mate flags only.
	 * 
	 * @param list
	 * @return
	 */
	private int detectCorrectedMateFlagsInSecondMember(List<SamRecordDiscrepancy> list) {
		int count = 0;
		for (SamRecordDiscrepancy d : list) {
			if (d.record1.getMateNegativeStrandFlag() != d.record2.getMateNegativeStrandFlag()
					|| d.record1.getMateUnmappedFlag() != d.record2.getMateUnmappedFlag())
				count++;
		}
		return count;
	}

	/**
	 * This is supposed to check if the mates have valid pairing flags.
	 * 
	 * @param r1
	 * @param r2
	 * @return
	 */
	private boolean checkMateFlags(SAMRecord r1, SAMRecord r2) {
		if (!r1.getReadPairedFlag() || !r2.getReadPairedFlag())
			return false;

		if (r1.getReadUnmappedFlag() != r2.getMateUnmappedFlag())
			return false;
		if (r1.getReadNegativeStrandFlag() != r2.getMateNegativeStrandFlag())
			return false;
		if (r1.getProperPairFlag() != r2.getProperPairFlag())
			return false;
		if (r1.getFirstOfPairFlag() && r2.getFirstOfPairFlag())
			return false;
		if (r1.getSecondOfPairFlag() && r2.getSecondOfPairFlag())
			return false;

		if (r2.getReadUnmappedFlag() != r1.getMateUnmappedFlag())
			return false;
		if (r2.getReadNegativeStrandFlag() != r1.getMateNegativeStrandFlag())
			return false;

		return true;
	}

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws SQLException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out.println("Failed to parse parameteres, detailed message below: ");
			System.out.println(e.getMessage());
			System.out.println();
			System.out.println("See usage: -h");
			System.exit(1);
		}

		if (args.length == 0 || params.help) {
			printUsage(jc);
			System.exit(1);
		}

		Log.setGlobalLogLevel(params.logLevel);

		if (params.referenceFasta != null)
			System.setProperty("reference", params.referenceFasta.getAbsolutePath());

		SAMFileReader.setDefaultValidationStringency(ValidationStringency.SILENT);
		SAMFileReader r1 = new SAMFileReader(params.file1);
		SAMFileReader r2 = new SAMFileReader(params.file2);

		SAMRecordIterator it1, it2;
		if (params.location != null) {
			AlignmentSliceQuery query = new AlignmentSliceQuery(params.location);
			if (SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(query.sequence)) {
				it1 = r1.queryUnmapped();
				it2 = r2.queryUnmapped();
			} else {
				it1 = r1.queryContained(query.sequence, query.start, query.end);
				it2 = r2.queryContained(query.sequence, query.start, query.end);
			}
		} else {
			it1 = r1.iterator();
			it2 = r2.iterator();
		}

		SamRecordComparision c = new SamRecordComparision();
		c.maxValueLen = params.maxValueLength;
		c.compareTags = params.compareTags;
		c.ignoreFlags = params.ignoreFalgs;
		c.ignoreTLENDiff = params.ignoreTLENDiff;
		c.maxValueLen = params.maxValueLength;

		if (params.ignoreTags != null) {
			String chunks[] = params.ignoreTags.split(":");
			for (String tagId : chunks) {
				if (!tagId.matches("^[A-Z]{2}$"))
					throw new RuntimeException("Expecting tag id to match ^[A-Z]{2}$ but got this: " + tagId);
				c.tagsToIgnore.add(tagId);
			}
		}

		if (params.ignoreFields != null) {
			String chunks[] = params.ignoreFields.split(":");
			for (String fieldName : chunks) {
				FIELD_TYPE type = FIELD_TYPE.valueOf(fieldName);
				c.fields.remove(type);
			}
		}

		if (params.scoreDiff) {
			int diffSize = 0;
			int recordsChecked = 0;

			int seqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
			byte[] ref = null;
			ReferenceSource source = new ReferenceSource(params.referenceFasta);
			List<PreservationPolicy> policies = params.lossySpec == null ? null : QualityScorePreservation
					.parsePolicies(params.lossySpec);

			while (diffSize < params.maxDiscrepancies && it1.hasNext()) {
				if (!it2.hasNext()) {
					System.err.println("file1 contains more reads than file2.");
					System.exit(1);
				}

				SAMRecord record1 = it1.next();
				SAMRecord record2 = it2.next();

				if (record1.getReferenceIndex() != seqId) {
					if (record1.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
						ref = new byte[0];
						seqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
					} else {
						if (record1.getReferenceIndex() != seqId) {
							seqId = record1.getReferenceIndex();
							SAMSequenceRecord s = r1.getFileHeader().getSequence(seqId);
							ref = source.getReferenceBases(s, true);
						}
					}
				}

				diffSize += dumpScoreDiffs(record1, record2, ref, policies);
				recordsChecked++;
			}
			System.out.printf("Checked %d records.", recordsChecked);
		} else {
			List<SamRecordDiscrepancy> discrepancies = c.compareRecords(it1, it2, params.maxDiscrepancies);

			r1.close();
			r2.close();

			if (params.countOnly)
				System.out.println(discrepancies.size());
			else if (params.dbDumpFile == null) {
				if (discrepancies.isEmpty())
					System.out.println("No discrepancies found");
				else {
					if (params.dumpDiscrepancies || params.dumpRecords) {
						for (SamRecordDiscrepancy d : discrepancies)
							c.log(d, params.dumpRecords, System.out);
					} else
						c.summary(discrepancies, System.out);

				}

			} else {
				db(params.dbDumpFile, "discrepancy".toUpperCase(), discrepancies.iterator());
			}

			if (!discrepancies.isEmpty())
				System.exit(1);
		}

	}

	private static void db(File dbFile, String tableName, Iterator<SamRecordDiscrepancy> it) throws SQLException {
		// Server server = Server.createTcpServer("").start();
		Connection connection = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath());
		createDiscrepancyTable(tableName, connection);
		dbLog(tableName, it, connection);
		connection.commit();
		connection.close();
	}

	private static class ScoreDiff {
		byte base, refBase, oScore, score;
		int posInRead;
		byte cigarOp, prevBaseCoP = -1, nextBaseCoP = -1;
		QualityScoreTreatmentType treatment = QualityScoreTreatmentType.DROP;
		int coverage = -1, pileup = -1;

		@Override
		public String toString() {
			return String.format("%d\t%c\t%c/%c\t%c/%c\t%s", posInRead, cigarOp, base, refBase, score + 33,
					oScore + 33, treatment.name());
		}
	}

	private static ScoreDiff[] findScoreDiffs(SAMRecord r1, SAMRecord r2, byte[] ref, List<PreservationPolicy> policies) {
		if (!r1.getCigarString().equals(r2.getCigarString()))
			throw new RuntimeException("CIGAR string are different.");

		List<ScoreDiff> diffs = new ArrayList<SamRecordComparision.ScoreDiff>();
		int posInRead = 1, posInRef = r1.getAlignmentStart();

		for (CigarElement ce : r1.getCigar().getCigarElements()) {
			if (ce.getOperator().consumesReadBases()) {
				for (int i = 0; i < ce.getLength(); i++) {
					if (r1.getBaseQualities()[i + posInRead - 1] != r2.getBaseQualities()[i + posInRead - 1]) {
						ScoreDiff d = new ScoreDiff();
						d.base = r1.getReadBases()[i + posInRead - 1];
						d.refBase = ref[i + posInRef - 1];
						ce.getOperator();
						d.cigarOp = CigarOperator.enumToCharacter(ce.getOperator());
						d.oScore = r1.getBaseQualities()[i + posInRead - 1];
						d.score = r2.getBaseQualities()[i + posInRead - 1];
						d.posInRead = i + posInRead;

						if (d.score == Binning.Illumina_binning_matrix[d.oScore])
							d.treatment = QualityScoreTreatmentType.BIN;
						else if (d.score == DEFAULT_SCORE)
							d.treatment = QualityScoreTreatmentType.DROP;

						if (policies == null || !obeysLossyModel(policies, r1, d))
							diffs.add(d);
					}
				}
			}

			posInRead += ce.getOperator().consumesReadBases() ? ce.getLength() : 0;
			posInRef += ce.getOperator().consumesReferenceBases() ? ce.getLength() : 0;
		}
		return diffs.toArray(new ScoreDiff[diffs.size()]);
	}

	private static int dumpScoreDiffs(SAMRecord r1, SAMRecord r2, byte[] ref, List<PreservationPolicy> policies) {
		ScoreDiff[] diffs = findScoreDiffs(r1, r2, ref, policies);
		if (diffs.length == 0)
			return 0;

		System.out.printf("%s\t%d\t%d\t%s:\n", r1.getReadName(), r1.getAlignmentStart(), r1.getMappingQuality(),
				(r1.getReadUnmappedFlag() ? "unmapped" : "mapped"));
		for (ScoreDiff diff : diffs) {
			System.out.printf("%s\n", diff.toString());
		}

		return diffs.length;
	}

	private static boolean testReadCategory(ReadCategory c, SAMRecord record) {
		switch (c.type) {
		case ALL:
			return true;
		case HIGHER_MAPPING_SCORE:
			return record.getMappingQuality() > c.param;
		case LOWER_MAPPING_SCORE:
			return record.getMappingQuality() < c.param;
		case UNPLACED:
			return record.getReadUnmappedFlag();

		default:
			throw new RuntimeException("Unknown read category: " + c.type.name());
		}
	}

	private static boolean testBaseCategory(BaseCategory c, ScoreDiff diff) {
		switch (c.type) {
		case FLANKING_DELETION:
			return diff.nextBaseCoP == CigarOperator.enumToCharacter(CigarOperator.DELETION)
					|| diff.prevBaseCoP == CigarOperator.enumToCharacter(CigarOperator.DELETION);
		case INSERTION:
			return diff.cigarOp == CigarOperator.enumToCharacter(CigarOperator.INSERTION);
		case LOWER_COVERAGE:
			return diff.coverage < c.param;
		case MATCH:
			return diff.base == diff.refBase;
		case MISMATCH:
			return diff.base != diff.refBase;
		case PILEUP:
			return diff.pileup <= c.param;

		default:
			throw new RuntimeException("Unknown read category: " + c.type.name());
		}
	}

	private static boolean testScoreTreatment(QualityScoreTreatment t, ScoreDiff diff) {
		switch (t.type) {
		case BIN:
			return diff.score == Binning.Illumina_binning_matrix[diff.oScore];
		case DROP:
			return diff.score == DEFAULT_SCORE;
		case PRESERVE:
			return diff.score == diff.oScore;

		default:
			throw new RuntimeException("Unknown quality score treatment category: " + t.type.name());
		}
	}

	private static boolean obeysLossyModel(List<PreservationPolicy> policies, SAMRecord record, ScoreDiff diff) {

		NEXT_POLICY: for (PreservationPolicy policy : policies) {
			if (policy.readCategory != null && !testReadCategory(policy.readCategory, record))
				continue;

			if (policy.baseCategories != null) {
				for (BaseCategory c : policy.baseCategories) {
					if (!testBaseCategory(c, diff))
						continue NEXT_POLICY;
				}
			}

			return testScoreTreatment(policy.treatment, diff);
		}

		return testScoreTreatment(QualityScoreTreatment.drop(), diff);
	}

	@Parameters(commandDescription = "Compare SAM/BAM/CRAM files.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--file1" }, converter = FileConverter.class, description = "First input file. ")
		File file1;
		@Parameter(names = { "--file2" }, converter = FileConverter.class, description = "Second input file. ")
		File file2;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file if required.")
		File referenceFasta;

		@Parameter(names = { "--max-discrepancies" }, description = "Stop after this many discrepancies found.")
		int maxDiscrepancies = Integer.MAX_VALUE;

		@Parameter(names = { "--max-value-len" }, description = "Trim all values to this length when reporting discrepancies.")
		int maxValueLength = 20;

		@Parameter(names = { "--location" }, description = "Compare reads only for this location, expected pattern: <seq name>:<from pos>-<to pos>")
		String location;

		@Parameter(names = { "--ignore-tags" }, description = "List of tags to ignore, for example: MD:NM:AM")
		String ignoreTags;

		@Parameter(names = { "--ignore-fields" }, description = "List of tags to ignore, for example: TLEN:CIGAR")
		String ignoreFields;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--count-only", "-c" }, description = "Report number of discrepancies only.")
		boolean countOnly = false;

		@Parameter(names = { "--compare-tags" }, description = "Compare tags.")
		boolean compareTags = false;

		@Parameter(names = { "--print-discrepancies" }, description = "Print out the discrepancies found, one per line.")
		boolean dumpDiscrepancies = false;

		@Parameter(names = { "--dump-conflicting-records" }, description = "Print out the records that differ.")
		boolean dumpRecords = false;

		@Parameter(names = { "--dump-to-db" }, description = "Dump the results into the specified database instead of the standard output. ")
		File dbDumpFile;

		@Parameter(names = { "--ignore-flags" }, description = "Ignore some bit flags. This should be an integer mask.")
		int ignoreFalgs = 0;

		@Parameter(names = { "--ignore-tlen-diff" }, description = "Ignore TLEN differences less of equal to this value.")
		int ignoreTLENDiff = 0;

		@Parameter(names = { "--score-diff", "-d" }, description = "Report differences only in base quality scores optionally using loosy spec.")
		boolean scoreDiff = false;

		@Parameter(names = { "--lossy-spec" }, description = "Test if quality scores in file2 obey this lossy spec.")
		String lossySpec;
	}
}
