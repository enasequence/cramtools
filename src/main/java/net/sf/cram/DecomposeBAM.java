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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.cram.structure.ReadTag;
import htsjdk.samtools.util.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

public class DecomposeBAM {
	private static Log log = Log.getInstance(DecomposeBAM.class);
	private static Params params;

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Bam2Cram.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		params = new Params();
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

		SAMFileReader reader = new SAMFileReader(params.bamFile);
		SAMRecordIterator iterator = reader.iterator();

		Map<String, Out> map = new TreeMap<String, DecomposeBAM.Out>();
		long bases = 0;
		while (iterator.hasNext() && params.maxRecords-- > 0) {
			SAMRecord record = iterator.next();
			bases += record.getReadLength();

			put(map, "QNAME", record.getReadName());
			put(map, "FLAG", String.valueOf(record.getFlags()));
			put(map, "RNAME", record.getReferenceName());
			put(map, "POS", String.valueOf(record.getAlignmentStart()));
			put(map, "MAPQ", String.valueOf(record.getMappingQuality()));
			put(map, "CIGAR", record.getCigarString());
			put(map, "RNEXT", record.getMateReferenceName());
			put(map, "PNEXT", String.valueOf(record.getMateAlignmentStart()));
			put(map, "TLEN", String.valueOf(record.getInferredInsertSize()));
			put(map, "SEQ", record.getReadString());
			put(map, "QUAL", record.getBaseQualityString());

			for (SAMRecord.SAMTagAndValue tv : record.getAttributes()) {
				writeTagValue(map, tv);
			}
		}

		close(map);
		iterator.close();
		reader.close();

		long totalBytes = 0;
		for (Out out : map.values())
			totalBytes += out.file.length();

		log.info(String.format("Bases %d, bytes %d, b/b %.2f\n", bases, totalBytes, 8f * totalBytes / bases));
	}

	private static void writeTagValue(Map<String, Out> map, SAMRecord.SAMTagAndValue tv) throws IOException {
		String name = tv.tag;
		char type = ReadTag.getTagValueType(tv.value);
		byte[] value = ReadTag.writeSingleValue((byte) type, tv.value, false);
		put(map, name, new String(value));
	}

	private static void close(Map<String, Out> map) throws IOException {
		for (Out out : map.values())
			out.close();
	}

	private static void put(Map<String, Out> map, String name, String value) throws IOException {
		Out out = map.get(name);
		if (out == null) {
			out = new Out(name, params.arith, params.model, params.snappy, params.raw);
			map.put(name, out);
		}

		out.write(value);
	}

	private static class Out {
		private File file;
		private OutputStream os;

		public Out(String name, boolean arith, String modelName, boolean snappy, boolean raw) throws IOException {
			if (raw) {
				this.file = new File(name + ".raw");
				FileOutputStream fos = new FileOutputStream(this.file);
				os = new BufferedOutputStream(fos);
			} else if (snappy) {
				throw new RuntimeException("Not implemented.");
				// this.file = new File(name + ".snappy");
				// FileOutputStream fos = new FileOutputStream(this.file);
				// SnappyOutputStream sos = new SnappyOutputStream(
				// new BufferedOutputStream(fos));
				// os = new BufferedOutputStream(sos);
			} else if (arith) {
				throw new RuntimeException("Not implemented.");

				// this.file = new File(name + ".a" + modelName);
				// FileOutputStream fos = new FileOutputStream(this.file);
				// ArithCodeModel model = null;
				// if ("A".equals(modelName))
				// model = new AdaptiveUnigramModel();
				// else if (modelName.startsWith("P")) {
				// int contextLength = 1;
				// if (modelName.length() > 1)
				// contextLength = Integer.valueOf(modelName.substring(1));
				//
				// model = new PPMModel(contextLength);
				// } else if ("U".equals(modelName))
				// model = UniformModel.MODEL;
				// else
				// throw new RuntimeException("Unknown model: " + modelName);
				//
				// os = new ArithCodeOutputStream(fos, model);
			} else {
				this.file = new File(name + ".gz");
				FileOutputStream fos = new FileOutputStream(this.file);
				GZIPOutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos));
				os = new BufferedOutputStream(gos);
			}
		}

		public void write(String value) throws IOException {
			os.write(value.getBytes());
		}

		public void close() throws IOException {
			os.close();
		}

	}

	@Parameters(commandDescription = "Decomposes BAM data series into separate files.")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "--input-bam-file", "-I" }, converter = FileConverter.class, description = "Path to the input BAM file.")
		File bamFile;

		@Parameter(names = { "--max-records" }, description = "Stop after compressing this many records. ")
		int maxRecords = Integer.MAX_VALUE;

		@Parameter
		List<String> sequences;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--arith" }, hidden = true, description = "Apply arithmetic coding.")
		boolean arith = false;

		@Parameter(names = { "--arith-model" }, hidden = true, description = "Choose coding model: A (adaptive), P (PPM), U(uniform).")
		String model = "A";

		@Parameter(names = { "--snappy" }, hidden = true, description = "Apply snappy compression.")
		boolean snappy = false;

		@Parameter(names = { "--raw" }, hidden = true, description = "Do not apply any compression.")
		boolean raw = false;
	}
}
