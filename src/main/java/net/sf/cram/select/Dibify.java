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
import java.sql.SQLException;
import java.util.Map;

import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.cram.structure.ReadTag;
import htsjdk.samtools.util.Log;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import net.sf.cram.AlignmentSliceQuery;
import net.sf.cram.CramTools;

public class Dibify {
	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Dibify.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) {
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

		if (params.referenceFasta != null)
			System.setProperty("reference", params.referenceFasta.getAbsolutePath());

		Log.setGlobalLogLevel(params.logLevel);

		SAMFileReader reader = null;
		if (params.file != null)
			reader = new SAMFileReader(params.file);
		else
			reader = new SAMFileReader(System.in);

		SAMRecordIterator it = null;
		if (params.location == null)
			it = reader.iterator();
		else {
			AlignmentSliceQuery query = new AlignmentSliceQuery(params.location);
			it = reader.query(query.sequence, query.start, query.end, false);
		}

		it = new BoundSAMRecordIterator(it, params.skipRecords, params.skipRecords + params.maxRecords);
		SAMFieldSelector s = new SAMFieldSelector(params.fields);
		SAMSubRecordStreamWriter writer = new SAMSubRecordStreamWriter();
		writer.selector = s;
		writer.ps = System.out;
		writer.enumerate = params.enumerate;
		long counter = params.skipRecords + 1;

		while (it.hasNext()) {
			SAMRecord record = it.next();
			writer.write(record, counter++);
		}

		it.close();
		reader.close();
	}

	public static interface SAMSubRecordWriter {
		public void write(SAMRecord record, long counter);
	}

	public static class SAMSubRecordStreamWriter implements SAMSubRecordWriter {
		public static String FIELD_SEPARATOR = "\t";
		private PrintStream ps;
		private SAMFieldSelector selector;
		private Map<SAMRecordField, Object> values;
		private boolean enumerate = false;

		@Override
		public void write(SAMRecord record, long counter) {
			values = selector.getValues(record, values);
			boolean first = true;

			if (enumerate) {
				ps.print(counter);
				first = false;
			}

			for (SAMRecordField f : SAMRecordField.SHARED) {
				if (f.type == FIELD_TYPE.TAG || !values.containsKey(f))
					continue;

				Object value = values.remove(f);
				if (!first)
					ps.print(FIELD_SEPARATOR);
				first = false;
				ps.print(SAMRecordField.toString(value));
			}

			for (SAMRecordField f : values.keySet()) {
				if (!first)
					ps.print(FIELD_SEPARATOR);
				first = false;

				Object value = values.get(f);
				ps.print(f.tagId);
				ps.print(':');
				ps.print(ReadTag.getTagValueType(value));
				ps.print(SAMRecordField.toString(value));
			}
			ps.print('\n');
		}
	}

	public static class H2Writer implements SAMSubRecordWriter {
		private SAMFieldSelector selector;
		private Map<SAMRecordField, Object> values;
		private Connection connection;
		private String tableName;

		public H2Writer(SAMFieldSelector selector, Connection connection, String tableName) throws SQLException {
			this.selector = selector;
			this.connection = connection;
			this.tableName = tableName;
			connection
					.prepareStatement(
							"CREATE TABLE "
									+ tableName
									+ "(counter INT PRIMARY KEY, field VARCHAR, tag VARCHAR, premature int, value1 VARCHAR, value2 VARCHAR, name1 VARCHAR, name2 VARCHAR, record1 VARCHAR, record2 VARCHAR);")
					.executeUpdate();
		}

		@Override
		public void write(SAMRecord record, long counter) {
			values = selector.getValues(record, values);

			// for (SAMRecordField f : SAMRecordField.SHARED) {
			// if (f.type == FIELD_TYPE.TAG || !values.containsKey(f))
			// continue;
			//
			// Object value = values.remove(f);
			// ps.print(SAMRecordField.toString(value));
			// }
			//
			// for (SAMRecordField f : values.keySet()) {
			// Object value = values.get(f);
			// ps.print(f.tagId);
			// ps.print(':');
			// ps.print(ReadTag.getTagValueType(value));
			// ps.print(SAMRecordField.toString(value));
			// }
			// ps.print('\n');
		}
	}

	@Parameters(commandDescription = "Provide SQL access to a SAM/BAM/CRAM file content.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-file", "-I" }, converter = FileConverter.class, description = "Path to a SAM/BAM/CRAM file. Omit if standard input (pipe).")
		File file;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "The reference fasta file, uncompressed and indexed (.fai file, use 'samtools faidx'). ")
		File referenceFasta;

		@Parameter(names = { "--output-file", "-O" }, converter = FileConverter.class, description = "The path for the output. Omit if standard output (pipe).")
		File outputFile = null;

		@Parameter(names = { "--max-records" }, description = "Stop after compressing this many records. ")
		int maxRecords = Integer.MAX_VALUE;

		@Parameter(names = { "--skip" }, description = "Skip first records.")
		int skipRecords = 0;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--h2" }, description = "Save the data in an H2 database.")
		boolean h2 = false;

		@Parameter(names = { "--cvs" }, description = "Save the data in CVS format.")
		boolean cvs = false;

		@Parameter(names = { "--execute" }, description = "An SQL statement to run against the data. ")
		String sql = null;

		@Parameter(names = { "--location" }, description = "Alignment location: <seq>:<from>-<to>")
		String location = null;

		@Parameter(names = { "--fields" }, description = "Specify what record field to load. Default it all.")
		String fields = "*";

		@Parameter(names = { "--enumerate" }, description = "Add sequential number to each record.")
		boolean enumerate = false;

	}
}
