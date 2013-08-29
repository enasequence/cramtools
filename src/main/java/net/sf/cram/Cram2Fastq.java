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
import net.sf.cram.encoding.reader.DataReaderFactory;
import net.sf.cram.encoding.reader.ReaderToFastq;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.ref.ReferenceSource;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMRecord;
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

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, IllegalAccessException,
			NoSuchAlgorithmException {
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
		ReferenceSource referenceSource = new ReferenceSource(params.reference);

		OutputStream joinedOS = null;
		OutputStream[] streams = null;
		File[] files = null;
		if (params.fastqBaseName == null) {
			joinedOS = System.out;
			if (params.gzip)
				joinedOS = (new GZIPOutputStream(joinedOS));
		} else {
			int maxFiles = 3;
			streams = new OutputStream[maxFiles];
			files = new File[maxFiles];
			String extension = ".fastq" + (params.gzip ? ".gz" : "");
			String path;
			for (int index = 0; index < streams.length; index++) {
				if (index == 0)
					path = params.fastqBaseName + extension;
				else
					path = params.fastqBaseName + "_" + index + extension;

				File file = new File(path);
				files[index] = file;
				OutputStream os = new BufferedOutputStream(
						new FileOutputStream(file));

				if (params.gzip)
					os = new GZIPOutputStream(os);

				streams[index] = os;
			}
		}

		byte[] ref = null;

		InputStream is = new FileInputStream(params.cramFile);

		CramHeader cramHeader = CramIO.readCramHeader(is);
		Container container = null;
		ReaderToFastq reader = new ReaderToFastq();
		while ((container = CramIO.readContainer(is)) != null) {
			DataReaderFactory f = new DataReaderFactory();

			for (Slice s : container.slices) {
				if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
					SAMSequenceRecord sequence = cramHeader.samFileHeader
							.getSequence(s.sequenceId);
					ref = referenceSource.getReferenceBases(sequence, true);

					if (!s.validateRefMD5(ref)) {
						log.error(String
								.format("Reference sequence MD5 mismatch for slice: seq id %d, start %d, span %d, expected MD5 %s\n",
										s.sequenceId, s.alignmentStart,
										s.alignmentSpan, String.format("%032x",
												new BigInteger(1, s.refMD5))));
						System.exit(1);
					}
				}
				Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (Integer exId : s.external.keySet()) {
					inputMap.put(exId,
							new ByteArrayInputStream(s.external.get(exId)
									.getRawContent()));
				}

				reader.referenceSequence = ref;
				reader.prevAlStart = s.alignmentStart;
				reader.substitutionMatrix = container.h.substitutionMatrix;
				reader.recordCounter = 0;
				reader.appendSegmentIndexToReadNames = params.appendSegmentIndexToReadNames;
				f.buildReader(reader, new DefaultBitInputStream(
						new ByteArrayInputStream(s.coreBlock.getRawContent())),
						inputMap, container.h, s.sequenceId);

				for (int i = 0; i < s.nofRecords; i++) {
					reader.read();
				}

				if (joinedOS != null) {
					for (ByteBuffer buf : reader.bufs) {
						buf.flip();
						joinedOS.write(buf.array(), 0, buf.limit());
						buf.clear();
					}
				} else {
					for (int i = 0; i < streams.length; i++) {
						ByteBuffer buf = reader.bufs[i];
						OutputStream os = streams[i];
						buf.flip();
						os.write(buf.array(), 0, buf.limit());
						if (buf.limit() > 0) files[i] = null ;
						buf.clear();
					}
				}
			}
		}

		if (streams != null) {
			for (OutputStream os : streams)
				if (os != null)
					os.close();
		}

		if (files != null) {
			for (File file : files)
				if (file != null)
					file.delete();

		}
	}

	@Parameters(commandDescription = "CRAM to BAM conversion. ")
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
	}

}
