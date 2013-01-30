package net.sf.cram;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecord.SAMTagAndValue;
import net.sf.samtools.SAMRecordIterator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class SamRecordComparision {
	private int maxValueLen = 15;
	private static Log log = Log.getInstance(SamRecordComparision.class);

	public enum FIELD {
		QNAME, FLAG, RNAME, POS, MAPQ, CIGAR, RNEXT, PNEXT, TLEN, SEQ, QUAL, TAG;
	}

	public EnumSet<FIELD> fields = EnumSet.allOf(FIELD.class);
	public Set<String> tagsToIgnore = new TreeSet<String>();
	public Set<String> tagsToCompare = new TreeSet<String>();
	public boolean compareTags = false;

	public static class SamRecordDiscrepancy {
		public FIELD field;
		public String tagId;
		public long recordCounter;

		public SAMRecord record1, record2;

		public int prematureEnd = 0;

	}

	public static Object getValue(SAMRecord record, FIELD field, String tagId) {
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
				throw new IllegalArgumentException(
						"Tag mismatch reqiues tag id. ");
			return record.getAttribute(tagId);

		default:
			throw new IllegalArgumentException("Unknown record field: "
					+ field.name());
		}
	}

	public boolean compareFieldValue(SAMRecord r1, SAMRecord r2, FIELD field,
			String tagId) {
		if (field == null)
			throw new IllegalArgumentException("Record field is null.");

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

		if (o1 instanceof SAMTagAndValue && o2 instanceof SAMTagAndValue) {
			SAMTagAndValue t1 = (SAMTagAndValue) o1;
			SAMTagAndValue t2 = (SAMTagAndValue) o2;

			return t1.tag.equals(t2.tag) && compareObjects(t1.value, t2.value);
		}

		return false;
	}

	public static String toString(Object value) {
		if (value == null)
			return null;
		if (value.getClass().isArray()) {

			if (value instanceof long[])
				return Arrays.toString((long[]) value);

			if (value instanceof int[])
				return Arrays.toString((int[]) value);

			if (value instanceof short[])
				return Arrays.toString((short[]) value);

			if (value instanceof byte[])
				return new String((byte[]) value);

			// this will fail if it is a primitive array:
			return Arrays.toString((Object[]) value);
		} else
			return value.toString();
	}

	private boolean compareTags(SAMRecord r1, SAMRecord r2, long recordCounter,
			List<SamRecordDiscrepancy> list) {
		if (!compareTags)
			return true;

		Map<String, SAMTagAndValue> m1 = new TreeMap<String, SAMRecord.SAMTagAndValue>();
		for (SAMTagAndValue t : r1.getAttributes())
			m1.put(t.tag, t);

		Map<String, SAMTagAndValue> m2 = new TreeMap<String, SAMRecord.SAMTagAndValue>();
		for (SAMTagAndValue t : r2.getAttributes())
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
			d.field = FIELD.TAG;
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
			d.field = FIELD.TAG;
			d.tagId = id;
			d.recordCounter = recordCounter;
			list.add(d);

			equal = false;
		}

		return equal;
	}

	public void compareRecords(SAMRecord r1, SAMRecord r2, long recordCounter,
			List<SamRecordDiscrepancy> list) {
		if (!r1.getReadName().equals(r2.getReadName())
				|| r1.getAlignmentStart() != r2.getAlignmentStart()) {
			System.err.println(r1.getSAMString());
			System.err.println(r2.getSAMString());
		}
		for (FIELD field : fields) {
			String tagId = null;
			if (field == FIELD.TAG) {
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

	public List<SamRecordDiscrepancy> compareRecords(SAMRecordIterator it1,
			SAMRecordIterator it2, int maxDiscrepandcies) {
		List<SamRecordDiscrepancy> discrepancies = new ArrayList<SamRecordComparision.SamRecordDiscrepancy>();
		long recordCounter = 0;

		while (it1.hasNext() && it2.hasNext()
				&& discrepancies.size() < maxDiscrepandcies) {
			recordCounter++;
			compareRecords(it1.next(), it2.next(), recordCounter, discrepancies);
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

	private static void createDiscrepancyTable(String tableName, Connection c)
			throws SQLException {
		System.out.println(tableName);
		PreparedStatement ps = c.prepareStatement("CREATE TABLE "
				+ tableName
				+ "(counter INT PRIMARY KEY, field VARCHAR, tag VARCHAR, premature int, value1 VARCHAR, value2 VARCHAR, name1 VARCHAR, name2 VARCHAR, record1 VARCHAR, record2 VARCHAR);");
		ps.executeUpdate() ;
		c.commit();
	}

	private static void dbLog(String tableName,
			Iterator<SamRecordDiscrepancy> it, Connection c)
			throws SQLException {
		PreparedStatement ps = c
				.prepareStatement("INSERT INTO "+tableName+" VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
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
				value1 = toString(getValue(d.record1, d.field, d.tagId));
				ps.setString(column++, value1);

				value2 = toString(getValue(d.record2, d.field, d.tagId));
				ps.setString(column++, value2);

				ps.setString(column++, d.record1.getReadName());
				ps.setString(column++, d.record2.getReadName());

				ps.setString(column++, d.record1.getSAMString());
				ps.setString(column++, d.record2.getSAMString());
				break;
			case 1:
				ps.setString(column++, null);

				value2 = toString(getValue(d.record2, FIELD.TAG, d.tagId));
				ps.setString(column++, value2);

				ps.setString(column++, null);
				ps.setString(column++, d.record2.getReadName());

				ps.setString(column++, null);
				ps.setString(column++, d.record2.getSAMString());
				break;
			case 2:
				value1 = toString(getValue(d.record1, FIELD.TAG, d.tagId));
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

	public void log(SamRecordDiscrepancy d, PrintStream ps) {
		switch (d.prematureEnd) {
		case 0:
			if (d.field != FIELD.TAG) {
				String value1 = toString(getValue(d.record1, d.field, null));
				String value2 = toString(getValue(d.record2, d.field, null));
				ps.println(String.format("FIELD:\t%d\t%s\t%s\t%s",
						d.recordCounter, d.field.name(),
						print(value1, maxValueLen), print(value2, maxValueLen)));
			} else {
				String value1 = toString(getValue(d.record1, FIELD.TAG, d.tagId));
				String value2 = toString(getValue(d.record2, FIELD.TAG, d.tagId));
				ps.println(String.format("TAG:\t%d\t%s\t%s\t%s\t%s",
						d.recordCounter, d.field.name(), d.tagId,
						print(value1, maxValueLen), print(value2, maxValueLen)));
			}
			ps.println("\t" + d.record1.getSAMString());
			ps.println("\t" + d.record2.getSAMString());
			break;
		case 1:
			ps.println(String.format("PREMATURE:\t%d\t%d", d.recordCounter,
					d.prematureEnd));
			ps.println("\t" + d.record2.getSAMString());
			break;
		case 2:
			ps.println(String.format("PREMATURE:\t%d\t%d", d.recordCounter,
					d.prematureEnd));
			ps.println("\t" + d.record1.getSAMString());
			break;

		default:
			throw new IllegalArgumentException("Unknown premature end value: "
					+ d.prematureEnd);
		}

	}

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws SQLException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		try {
			jc.parse(args);
		} catch (Exception e) {
			System.out
					.println("Failed to parse parameteres, detailed message below: ");
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
			System.setProperty("reference",
					params.referenceFasta.getAbsolutePath());

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

		if (params.ignoreTags != null) {
			String chunks[] = params.ignoreTags.split(":");
			for (String tagId : chunks) {
				if (!tagId.matches("^[A-Z]{2}$"))
					throw new RuntimeException(
							"Expecting tag id to match ^[A-Z]{2}$ but got this: "
									+ tagId);
				c.tagsToIgnore.add(tagId);
			}
		}

		List<SamRecordDiscrepancy> discrepancies = c.compareRecords(it1, it2,
				params.maxDiscrepancies);

		if (params.countOnly)
			System.out.println(discrepancies.size());
		else if (params.dbDumpFile == null) {
			for (SamRecordDiscrepancy d : discrepancies)
				c.log(d, System.out);
		} else {
			db(params.dbDumpFile,
					"discrepancy".toUpperCase(),
					discrepancies.iterator());
		}

		r1.close();
		r2.close();

	}

	private static void db(File dbFile, String tableName,
			Iterator<SamRecordDiscrepancy> it) throws SQLException {
		// Server server = Server.createTcpServer("").start();
		Connection connection = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath());
		createDiscrepancyTable(tableName, connection);
		dbLog(tableName, it, connection);
		connection.commit();
		connection.close();
	}

	@Parameters(commandDescription = "Compare SAM/BAM/CRAM files.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "--file1" }, converter = FileConverter.class, description = "First input file. ")
		File file1;
		@Parameter(names = { "--file2" }, converter = FileConverter.class, description = "Second input file. ")
		File file2;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file if required.")
		File referenceFasta;

		@Parameter(names = { "--max-discrepancies" }, description = "Stop after this many discrepancies found.")
		int maxDiscrepancies = Integer.MAX_VALUE;

		@Parameter(names = { "--max-value-len" }, description = "Trim all values to this length when reporting discrepancies.")
		int maxValueLength = 10;

		@Parameter(names = { "--location" }, description = "Compare reads only for this location, expected pattern: <seq name>:<from pos>-<to pos>")
		String location;

		@Parameter(names = { "--ignore-tags" }, description = "List of tags to ignore, for example: MD:NM:AM")
		String ignoreTags;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--count-only", "-c" }, description = "Report number of discrepancies only.")
		boolean countOnly = false;

		@Parameter(names = { "--compare-tags" }, description = "Compare tags.")
		boolean compareTags = false;

		@Parameter(names = { "--dump-conflicting-records" }, description = "Print out the records that differ.")
		boolean dumpRecords = false;

		@Parameter(names = { "--dump-to-db" }, description = "Dump the results into the specified database instead of the standard output. ")
		File dbDumpFile;
	}
}
