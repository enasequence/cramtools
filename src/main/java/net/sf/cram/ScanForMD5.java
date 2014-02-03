package net.sf.cram;

import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import net.sf.cram.common.Utils;
import net.sf.cram.ref.ReferenceSource;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.SAMSequenceRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class ScanForMD5 {
	private byte[] bases;
	private ByteBuffer buf;

	public ScanForMD5(byte[] bases, int bufSize) {
		this.bases = bases;
		this.buf = ByteBuffer.allocate(bufSize);
	}

	public void test(TestResult t) {
		buf.clear();
		buf.put(bases, t.start - 1, t.length);
		buf.flip();

		for (TREAT treat : t.treats)
			apply(treat);

		t.md5 = md5(buf);
	}

	public void apply(TREAT treat) {
		switch (treat) {
		case UC:
			uc(buf);
			break;
		case REPLACE:
			replaceNonACGTN(buf);
			break;
		default:
			throw new RuntimeException();
		}
	}

	public enum TREAT {
		UC, REPLACE;
	}

	private static class TestResult {
		int start, length;
		List<TREAT> treats;
		String md5;
	}

	public static void main(String[] args) {
		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.parse(args);

		ReferenceSequenceFile r = ReferenceSequenceFileFactory.getReferenceSequenceFile(params.fastaFile);
		AlignmentSliceQuery query = new AlignmentSliceQuery(params.query);

		byte[] bases = r.getSequence(query.sequence).getBases();

		ReferenceSource source = new ReferenceSource(params.fastaFile);
		SAMSequenceRecord record = new SAMSequenceRecord(query.sequence, bases.length);
		byte[] bases2 = source.getReferenceBases(record, true);
		Utils.upperCase(bases2);
		int span = Math.min(query.end - query.start + 1, bases2.length - query.start + 1);
		System.out.println(Utils.calculateMD5String(bases2, query.start - 1, span));

		ScanForMD5 scan = new ScanForMD5(bases, query.end - query.start + 100);
		for (int start = query.start - 1 - 10; start <= query.start + 1 + 10; start++) {
			for (int len = query.end - query.start + 1 - 10; len <= query.end - query.start + 1 + 10; len++) {
				if (start + len > bases.length)
					continue;

				if (test(params.md5, scan, start, len, new TREAT[] {}))
					return;
				if (test(params.md5, scan, start, len, new TREAT[] { TREAT.UC }))
					return;
				if (test(params.md5, scan, start, len, new TREAT[] { TREAT.REPLACE }))
					return;
				if (test(params.md5, scan, start, len, new TREAT[] { TREAT.UC, TREAT.REPLACE }))
					return;
				if (test(params.md5, scan, start, len, new TREAT[] { TREAT.REPLACE, TREAT.UC }))
					return;
			}
		}
	}

	private static boolean test(String expectedMD5, ScanForMD5 scan, int start, int length, TREAT... treats) {
		TestResult test = new TestResult();
		test.start = start;
		test.length = length;
		test.treats = Arrays.asList(treats);
		scan.test(test);

		if (expectedMD5.equals(test.md5)) {
			System.out.printf("found: start %d, end %d, length %d, treats: %s.\n", start, start + length - 1, length,
					Arrays.toString(treats));
			return true;
		}

		// System.out.printf("trying: %s -> start %d, end %d, length %d, treats: %s.\n",
		// test.md5, start, start + length
		// - 1, length, Arrays.toString(treats));
		return false;
	}

	private static void uc(ByteBuffer buf) {
		byte[] array = buf.array();
		for (int i = 0; i < array.length; i++) {
			byte b = array[i];
			if (b >= 'a' && b <= 'z')
				array[i] -= 32;
		}
	}

	private static void replaceNonACGTN(ByteBuffer buf) {
		byte[] array = buf.array();
		for (int i = 0; i < array.length; i++) {
			byte b = array[i];
			switch (b) {
			case 'A':
			case 'C':
			case 'G':
				break;
			default:
				array[i] = 'N';
			}
		}
	}

	private static String md5(ByteBuffer buf) {
		MessageDigest md5_MessageDigest;
		byte[] digest;
		try {
			md5_MessageDigest = MessageDigest.getInstance("MD5");
			md5_MessageDigest.reset();

			md5_MessageDigest.update(buf);
			digest = md5_MessageDigest.digest();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		return String.format("%032x", new BigInteger(1, digest));
	}

	@Parameters
	static class Params {
		@Parameter(names = { "-F" }, converter = FileConverter.class)
		File fastaFile;

		@Parameter(names = { "-Q" })
		String query;

		@Parameter(names = { "-M" })
		String md5;
	}

}
