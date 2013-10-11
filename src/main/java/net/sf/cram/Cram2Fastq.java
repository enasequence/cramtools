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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import net.sf.cram.CramTools.LevelConverter;
import net.sf.cram.build.CramIO;
import net.sf.cram.encoding.reader.AbstractFastqReader;
import net.sf.cram.encoding.reader.DataReaderFactory;
import net.sf.cram.encoding.reader.MultiFastqOutputter;
import net.sf.cram.encoding.reader.ReaderToFastq;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Cram2Fastq {
	private static Log log = Log.getInstance(Cram2Fastq.class);
	public static final String COMMAND = "fastq";

	private static void printUsage(JCommander jc) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		jc.usage(sb);

		System.out.println("Version "
				+ Cram2Fastq.class.getPackage().getImplementationVersion());
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws Exception {
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

		if (params.reference == null)
			log.warn("No reference file specified, remote access over internet may be used to download public sequences. ");

		// SimpleDumper d = new SimpleDumper(new
		// FileInputStream(params.cramFile),
		// new ReferenceSource(params.reference), 3, params.fastqBaseName,
		// params.gzip, params.maxParams);

		CollatingDumper d = new CollatingDumper(new FileInputStream(
				params.cramFile), new ReferenceSource(params.reference), 3,
				params.fastqBaseName, params.gzip, params.maxRecords);
		d.run();

		if (d.exception != null)
			throw d.exception;
	}

	private static abstract class Dumper implements Runnable {
		protected InputStream cramIS;
		protected byte[] ref = null;
		protected ReferenceSource referenceSource;
		protected FileOutput[] outputs;
		protected long maxRecords = -1;
		protected CramHeader cramHeader;
		protected Container container;
		protected AbstractFastqReader reader;
		protected Exception exception;

		public Dumper(InputStream cramIS, ReferenceSource referenceSource,
				int nofStreams, String fastqBaseName, boolean gzip,
				long maxRecords) throws IOException {

			this.cramIS = cramIS;
			this.referenceSource = referenceSource;
			this.maxRecords = maxRecords;
			outputs = new FileOutput[nofStreams];
			for (int index = 0; index < outputs.length; index++)
				outputs[index] = new FileOutput();

			if (fastqBaseName == null) {
				OutputStream joinedOS = System.out;
				if (gzip)
					joinedOS = (new GZIPOutputStream(joinedOS));
				for (int index = 0; index < outputs.length; index++)
					outputs[index].outputStream = joinedOS;
			} else {
				String extension = ".fastq" + (gzip ? ".gz" : "");
				String path;
				for (int index = 0; index < outputs.length; index++) {
					if (index == 0)
						path = fastqBaseName + extension;
					else
						path = fastqBaseName + "_" + index + extension;

					outputs[index].file = new File(path);
					OutputStream os = new BufferedOutputStream(
							new FileOutputStream(outputs[index].file));

					if (gzip)
						os = new GZIPOutputStream(os);

					outputs[index].outputStream = os;
				}
			}
		}

		protected abstract AbstractFastqReader newReader();

		protected abstract void containerHasBeenRead() throws IOException;

		protected void doRun() throws IOException {
			cramHeader = CramIO.readCramHeader(cramIS);
			reader = newReader();
			MAIN_LOOP: while ((container = CramIO.readContainer(cramIS)) != null) {
				DataReaderFactory f = new DataReaderFactory();

				for (Slice s : container.slices) {
					if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
						SAMSequenceRecord sequence = cramHeader.samFileHeader
								.getSequence(s.sequenceId);
						ref = referenceSource.getReferenceBases(sequence, true);

						try {
							if (!s.validateRefMD5(ref)) {
								log.error(String
										.format("Reference sequence MD5 mismatch for slice: seq id %d, start %d, span %d, expected MD5 %s\n",
												s.sequenceId, s.alignmentStart,
												s.alignmentSpan, String.format(
														"%032x",
														new BigInteger(1,
																s.refMD5))));
								throw new RuntimeException(
										"Reference checksum mismatch.");
							}
						} catch (NoSuchAlgorithmException e) {
							throw new RuntimeException(e);
						}
					}
					Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
					for (Integer exId : s.external.keySet()) {
						inputMap.put(exId, new ByteArrayInputStream(s.external
								.get(exId).getRawContent()));
					}

					reader.referenceSequence = ref;
					reader.prevAlStart = s.alignmentStart;
					reader.substitutionMatrix = container.h.substitutionMatrix;
					reader.recordCounter = 0;
					try {
						f.buildReader(
								reader,
								new DefaultBitInputStream(
										new ByteArrayInputStream(s.coreBlock
												.getRawContent())), inputMap,
								container.h, s.sequenceId);
					} catch (IllegalArgumentException e) {
						throw new RuntimeException(e);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}

					for (int i = 0; i < s.nofRecords; i++) {
						reader.read();
						if (maxRecords > -1) {
							if (maxRecords == 0)
								break MAIN_LOOP;
							maxRecords--;
						}
					}

					containerHasBeenRead();
				}
			}
			reader.finish();
		}

		@Override
		public void run() {
			try {
				doRun();

				if (outputs != null) {
					for (FileOutput os : outputs)
						os.close();
				}
			} catch (Exception e) {
				this.exception = e;
			}
		}
	}

	private static class SimpleDumper extends Dumper {
		public SimpleDumper(InputStream cramIS,
				ReferenceSource referenceSource, int nofStreams,
				String fastqBaseName, boolean gzip, int maxRecords)
				throws IOException {
			super(cramIS, referenceSource, nofStreams, fastqBaseName, gzip,
					maxRecords);
		}

		@Override
		protected AbstractFastqReader newReader() {
			return new ReaderToFastq();
		}

		@Override
		protected void containerHasBeenRead() throws IOException {
			ReaderToFastq reader = (ReaderToFastq) super.reader;
			for (int i = 0; i < outputs.length; i++) {
				ByteBuffer buf = reader.bufs[i];
				OutputStream os = outputs[i].outputStream;
				buf.flip();
				os.write(buf.array(), 0, buf.limit());
				if (buf.limit() > 0)
					outputs[i].empty = false;
				buf.clear();
			}
		}
	}

	private static class CollatingDumper extends Dumper {
		private FileOutput fo = new FileOutput();

		public CollatingDumper(InputStream cramIS,
				ReferenceSource referenceSource, int nofStreams,
				String fastqBaseName, boolean gzip, long maxRecords)
				throws IOException {
			super(cramIS, referenceSource, nofStreams, fastqBaseName, gzip,
					maxRecords);
			fo.file = new File(fastqBaseName == null ? "overflow.bam"
					: fastqBaseName + ".overflow.bam");
			fo.outputStream = new BufferedOutputStream(new FileOutputStream(
					fo.file));
		}

		@Override
		protected AbstractFastqReader newReader() {
			return new MultiFastqOutputter(outputs, fo);
		}

		@Override
		protected void containerHasBeenRead() throws IOException {
		}

		@Override
		public void doRun() throws IOException {
			super.doRun();

			fo.close();

			if (fo.empty)
				return;

			log.info("Sorting overflow BAM: ", fo.file.length());
			SAMFileReader
					.setDefaultValidationStringency(ValidationStringency.SILENT);
			SAMFileReader r = new SAMFileReader(fo.file);
			SAMRecordIterator iterator = r.iterator();
			if (!iterator.hasNext()) {
				r.close();
				fo.file.delete();
				return;
			}

			SAMRecord r1 = iterator.next();
			SAMRecord r2 = null;
			while (iterator.hasNext()) {
				r2 = iterator.next();
				if (r1.getReadName().equals(r2.getReadName())) {
					print(r1, r2);
					if (!iterator.hasNext())
						break;
					r1 = iterator.next();
					r2 = null;
				} else {
					print(r1, 0);
					r1 = r2;
					r2 = null;
				}
			}
			if (r1 != null)
				print(r1, 0);
			r.close();
			fo.file.delete();
		}

		private void print(SAMRecord r1, SAMRecord r2) throws IOException {
			if (r1.getFirstOfPairFlag()) {
				print(r1, 1);
				print(r2, 2);
			} else {
				print(r1, 2);
				print(r2, 1);
			}
		}

		private void print(SAMRecord r, int index) throws IOException {
			OutputStream os = outputs[index].outputStream;
			os.write('@');
			os.write(r.getReadName().getBytes());
			if (index > 0) {
				os.write('/');
				os.write(48 + index);
			}
			os.write('\n');
			os.write(r.getReadBases());
			os.write("\n+\n".getBytes());
			os.write(r.getBaseQualityString().getBytes());
			os.write('\n');
		}
	}

	private static class FileOutput extends OutputStream {
		File file;
		OutputStream outputStream;
		boolean empty = true;

		@Override
		public void write(int b) throws IOException {
			outputStream.write(b);
			empty = false;
		}

		@Override
		public void write(byte[] b) throws IOException {
			outputStream.write(b);
			empty = false;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			outputStream.write(b, off, len);
			empty = false;
		}

		@Override
		public void flush() throws IOException {
			outputStream.flush();
		}

		@Override
		public void close() throws IOException {
			if (outputStream != null && outputStream != System.out
					&& outputStream != System.err) {
				outputStream.close();
				outputStream = null;
			}
			if (empty && file != null && file.exists())
				file.delete();
		}
	}

	@Parameters(commandDescription = "CRAM to FastQ dump conversion. ")
	static class Params {
		@Parameter(names = { "-l", "--log-level" }, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = LevelConverter.class)
		LogLevel logLevel = LogLevel.ERROR;

		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		boolean help = false;

		@Parameter(names = { "--input-cram-file", "-I" }, converter = FileConverter.class, description = "The path to the CRAM file to uncompress. Omit if standard input (pipe).")
		File cramFile;

		@Parameter(names = { "--reference-fasta-file", "-R" }, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example). ")
		File reference;

		@Parameter(names = { "--fastq-base-name", "-F" }, description = "'_number.fastq[.gz] will be appended to this string to obtain output fastq file name. If this parameter is omitted then all reads are printed with no garanteed order.")
		String fastqBaseName;

		@Parameter(names = { "--gzip", "-z" }, description = "Compress fastq files with gzip.")
		boolean gzip;

		@Parameter(names = { "--reverse" }, description = "Re-reverse reads mapped to negative strand.")
		boolean reverse;

		@Parameter(names = { "--enumerate" }, description = "Append read names with read index (/1 for first in pair, /2 for second in pair).")
		boolean appendSegmentIndexToReadNames;

		@Parameter(names = { "--max-records" }, description = "Stop after reading this many records.")
		long maxRecords = -1;

		@Parameter(names = { "--collate" }, description = "Read name collation.")
		boolean collate = false;
	}

}
