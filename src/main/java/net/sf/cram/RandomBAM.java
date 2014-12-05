package net.sf.cram;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import net.sf.cram.common.Utils;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class RandomBAM {

	public static void main(String[] args) throws NoSuchAlgorithmException {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		byte[] ref;
		ReferenceSequence sequence;
		if (params.refFile != null) {
			ReferenceSequenceFile rsf = ReferenceSequenceFileFactory.getReferenceSequenceFile(params.refFile);
			sequence = rsf.nextSequence();
			ref = sequence.getBases();
		} else {

			ref = new byte[100000];
			Arrays.fill(ref, (byte) 'A');
			sequence = new ReferenceSequence("1", 0, ref);
		}

		byte[] scores = new byte[params.readLength];
		for (int i = 0; i < scores.length; i++)
			scores[i] = (byte) (40 - Math.sqrt((float) i / scores.length) * 20);

		String refMD5 = Utils.calculateMD5String(ref);
		SAMSequenceRecord sequenceRecord = new SAMSequenceRecord(sequence.getName(), sequence.length());
		sequenceRecord.setAttribute(SAMSequenceRecord.MD5_TAG, refMD5);

		SAMFileHeader header = new SAMFileHeader();
		header.setSortOrder(SortOrder.coordinate);
		header.addSequence(sequenceRecord);
		SAMFileWriter writer;
		if (params.outputFile == null)
			writer = new SAMFileWriterFactory().makeSAMWriter(header, true, System.out);
		else
			writer = new SAMFileWriterFactory().makeSAMWriter(header, true, params.outputFile);

		Random random = new Random();
		int refPos = 0;
		for (int i = 0; i < params.readCount; i++) {
			int readLen = params.readLength;
			SAMRecord record = new SAMRecord(header);
			record.setReferenceName(sequence.getName());
			record.setReadName(Integer.toString(i + 1));
			record.setCigarString(readLen + "M");
			refPos += random.nextInt(10);
			record.setReadUnmappedFlag(false);
			record.setAlignmentStart(refPos + 1);
			byte[] b = Arrays.copyOfRange(ref, refPos, refPos + readLen);
			byte[] choice = "ACGTN".getBytes();
			for (int j = 0; j < b.length; j++) {
				if (random.nextInt(1000) < 60)
					b[j] = choice[random.nextInt(choice.length)];
			}
			record.setReadBases(b);

			byte[] s = Arrays.copyOf(scores, readLen);
			for (int j = 0; j < s.length; j++) {
				if (random.nextInt(1000) < 300)
					s[j] = (byte) random.nextInt(40);
			}
			record.setBaseQualities(s);

			writer.addAlignment(record);
		}
		writer.close();
	}

	@Parameters
	static class Params {
		@Parameter(names = { "-O" }, converter = FileConverter.class)
		File outputFile;

		@Parameter(names = { "-R" }, converter = FileConverter.class)
		File refFile;

		@Parameter(names = { "--reads" })
		int readCount = 10;

		@Parameter(names = { "--read-len" })
		int readLength = 1000;
	}
}
