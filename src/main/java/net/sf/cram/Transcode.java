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

package net.sf.cram;

import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.CramTools.ValidationStringencyConverter;
import net.sf.cram.ref.ReferenceSource;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Transcode {
	private static Log log = Log.getInstance(Transcode.class);

	public static void usage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version " + Merge.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		Log.setGlobalLogLevel(params.logLevel);

		if (args.length == 0 || params.help) {
			usage(jc);
			System.exit(1);
		}

		if (params.reference == null) {
			System.out.println("Reference file not found, will try downloading...");
		}

		ReferenceSource referenceSource = null;
		if (params.reference != null) {
			System.setProperty("reference", params.reference.getAbsolutePath());
			referenceSource = new ReferenceSource(params.reference);
		} else {
			String prop = System.getProperty("reference");
			if (prop != null) {
				referenceSource = new ReferenceSource(new File(prop));
			}
		}

		SamReaderFactory factory = SamReaderFactory.make().validationStringency(params.validationLevel);
		SamInputResource r;
		if ("file".equalsIgnoreCase(params.url.getProtocol()))
			r = SamInputResource.of(params.url.getPath());
		else
			r = SamInputResource.of(params.url);
		SamReader reader = factory.open(r);
		SAMRecordIterator iterator = reader.iterator();

		SAMFileWriterFactory writerFactory = new SAMFileWriterFactory();
		SAMFileWriter writer = null;
		OutputStream os = new BufferedOutputStream(new FileOutputStream(params.outputFile));
		switch (params.outputFormat) {
		case BAM:
			writer = writerFactory.makeBAMWriter(reader.getFileHeader(),
					reader.getFileHeader().getSortOrder() == SortOrder.coordinate, os);
			break;
		case CRAM:
			writer = writerFactory.makeCRAMWriter(reader.getFileHeader(), os, params.reference);
			break;

		default:
			System.out.println("Unknown output format: " + params.outputFormat);
			System.exit(1);
		}

		while (iterator.hasNext()) {
			writer.addAlignment(iterator.next());
		}
		writer.close();
		reader.close();
	}

	public enum FORMAT {
		UNKNOWN, BAM, CRAM;
	}

	public static class FormatConverter implements IStringConverter<FORMAT> {

		@Override
		public FORMAT convert(String f) {
			if (FORMAT.BAM.name().equalsIgnoreCase(f))
				return FORMAT.BAM;
			if (FORMAT.CRAM.name().equalsIgnoreCase(f))
				return FORMAT.CRAM;

			return FORMAT.UNKNOWN;
		}

	}

	public static class URLConverter implements IStringConverter<URL> {

		@Override
		public URL convert(String f) {
			URL url;
			try {
				return new URL(f);
			} catch (MalformedURLException e) {
				File file = new File(f);
				if (file.exists())
					try {
						return file.toURI().toURL();
					} catch (MalformedURLException e1) {
						throw new RuntimeException(e1);
					}
				throw new RuntimeException("Malformed URL: " + f);
			}

		}

	}

	@Parameters(commandDescription = "Tool to merge CRAM or BAM files. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		Log.LogLevel logLevel = Log.LogLevel.ERROR;

		@Parameter(names = { "-v", "--validation-level" }, description = "Change validation stringency level: STRICT, LENIENT, SILENT.", converter = ValidationStringencyConverter.class)
		ValidationStringency validationLevel = ValidationStringency.DEFAULT_STRINGENCY;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example).")
		File reference;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "-i", "--input-format" }, converter = FormatConverter.class, description = "Input file format")
		FORMAT inputFormat;

		@Parameter(names = { "-o", "--output-format" }, converter = FormatConverter.class, description = "Output file format")
		FORMAT outputFormat;

		@Parameter(names = { "-I", "--input" }, converter = URLConverter.class, description = "Input URL")
		URL url;

		@Parameter(names = { "-O", "--output-file" }, converter = FileConverter.class, description = "Output file")
		File outputFile;
	}
}
