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

package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.RuntimeIOException;
import htsjdk.samtools.util.StringLineReader;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class SAMFileHeader_Utils {
	static SAMFileHeader readHeader(final BinaryCodec stream, final ValidationStringency validationStringency,
			final String source) throws IOException {

		final byte[] buffer = new byte[4];
		stream.readBytes(buffer);
		if (!Arrays.equals(buffer, "BAM\1".getBytes())) {
			throw new IOException("Invalid BAM file header");
		}

		final int headerTextLength = stream.readInt();
		final String textHeader = stream.readString(headerTextLength);
		final SAMTextHeaderCodec headerCodec = new SAMTextHeaderCodec();
		headerCodec.setValidationStringency(validationStringency);
		final SAMFileHeader samFileHeader = headerCodec.decode(new StringLineReader(textHeader), source);

		final int sequenceCount = stream.readInt();
		if (samFileHeader.getSequenceDictionary().size() > 0) {
			// It is allowed to have binary sequences but no text sequences, so
			// only validate if both are present
			if (sequenceCount != samFileHeader.getSequenceDictionary().size()) {
				throw new SAMFormatException("Number of sequences in text header ("
						+ samFileHeader.getSequenceDictionary().size() + ") != number of sequences in binary header ("
						+ sequenceCount + ") for file " + source);
			}
			for (int i = 0; i < sequenceCount; i++) {
				final SAMSequenceRecord binarySequenceRecord = readSequenceRecord(stream, source);
				final SAMSequenceRecord sequenceRecord = samFileHeader.getSequence(i);
				if (!sequenceRecord.getSequenceName().equals(binarySequenceRecord.getSequenceName())) {
					throw new SAMFormatException("For sequence " + i
							+ ", text and binary have different names in file " + source);
				}
				if (sequenceRecord.getSequenceLength() != binarySequenceRecord.getSequenceLength()) {
					throw new SAMFormatException("For sequence " + i
							+ ", text and binary have different lengths in file " + source);
				}
			}
		} else {
			// If only binary sequences are present, copy them into
			// samFileHeader
			final List<SAMSequenceRecord> sequences = new ArrayList<SAMSequenceRecord>(sequenceCount);
			for (int i = 0; i < sequenceCount; i++) {
				sequences.add(readSequenceRecord(stream, source));
			}
			samFileHeader.setSequenceDictionary(new SAMSequenceDictionary(sequences));
		}

		return samFileHeader;
	}

	private static SAMSequenceRecord readSequenceRecord(final BinaryCodec stream, final String source) {
		final int nameLength = stream.readInt();
		if (nameLength <= 1) {
			throw new SAMFormatException("Invalid BAM file header: missing sequence name in file " + source);
		}
		final String sequenceName = stream.readString(nameLength - 1);
		// Skip the null terminator
		stream.readByte();
		final int sequenceLength = stream.readInt();
		return new SAMSequenceRecord(SAMSequenceRecord.truncateSequenceName(sequenceName), sequenceLength);
	}

	static void writeHeader(final BinaryCodec outputBinaryCodec, final SAMFileHeader samFileHeader,
			final String headerText) {
		outputBinaryCodec.writeBytes("BAM\1".getBytes());

		// calculate and write the length of the SAM file header text and the
		// header text
		outputBinaryCodec.writeString(headerText, true, false);

		// write the sequences binarily. This is redundant with the text header
		outputBinaryCodec.writeInt(samFileHeader.getSequenceDictionary().size());
		for (final SAMSequenceRecord sequenceRecord : samFileHeader.getSequenceDictionary().getSequences()) {
			outputBinaryCodec.writeString(sequenceRecord.getSequenceName(), true, true);
			outputBinaryCodec.writeInt(sequenceRecord.getSequenceLength());
		}
	}

	/**
	 * Writes a header to a BAM file. Might need to regenerate the String
	 * version of the header, if one already has both the samFileHeader and the
	 * String, use the version of this method which takes both.
	 */
	static void writeHeader(final BinaryCodec outputBinaryCodec, final SAMFileHeader samFileHeader) {
		// Do not use SAMFileHeader.getTextHeader() as it is not updated when
		// changes to the underlying object are made
		final String headerString;
		final Writer stringWriter = new StringWriter();
		new SAMTextHeaderCodec().encode(stringWriter, samFileHeader, true);
		headerString = stringWriter.toString();

		writeHeader(outputBinaryCodec, samFileHeader, headerString);
	}

	protected static void writeHeader(final OutputStream outputStream, final SAMFileHeader samFileHeader) {
		final BlockCompressedOutputStream blockCompressedOutputStream = new BlockCompressedOutputStream(outputStream,
				null);
		final BinaryCodec outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));
		writeHeader(outputBinaryCodec, samFileHeader);
		try {
			blockCompressedOutputStream.flush();
		} catch (final IOException ioe) {
			throw new RuntimeIOException(ioe);
		}
	}
}
