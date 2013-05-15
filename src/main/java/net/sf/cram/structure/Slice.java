package net.sf.cram.structure;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import net.sf.cram.common.Utils;
import net.sf.picard.util.Log;
import net.sf.samtools.util.RuntimeEOFException;

public class Slice {
	private static final Log log = Log.getInstance(Slice.class);

	// as defined in the specs:
	public int sequenceId = -1;
	public int alignmentStart = -1;
	public int alignmentSpan = -1;
	public int nofRecords = -1;
	public long globalRecordCounter = -1;
	public int nofBlocks = -1;
	public int[] contentIDs;
	public int embeddedRefBlockContentID = -1;
	public byte[] refMD5;

	// content associated with ids:
	public Block headerBlock;
	public BlockContentType contentType;
	public Block coreBlock;
	public Block embeddedRefBlock;
	public Map<Integer, Block> external;

	// for indexing purposes:
	public int offset = -1;
	public long containerOffset = -1;
	public int size = -1;
	public int index = -1;

	// to pass this to the container:
	public long bases;

	// private static int sliceCounter = 0 ;
	// public final int sliceId = createSliceId() ;
	//
	// private static synchronized int createSliceId() {
	// return sliceCounter++ ;
	// }

	public boolean validateRefMD5(byte[] ref) {
		try {
			int span = Math.min(alignmentSpan, ref.length - alignmentStart + 1);
//			System.out.println(new String (ref, alignmentStart-1, span));
			String md5 = Utils.calculateMD5(ref, alignmentStart - 1,span);
			String sliceMD5 = String.format("%032x", new BigInteger(1, refMD5));
			if (!md5.equals(sliceMD5)) {
				StringBuffer sb = new StringBuffer();
				int shoulder = 10;
				sb.append(new String(Arrays.copyOfRange(ref, alignmentStart-1,
						alignmentStart + shoulder)));
				sb.append("...");
				sb.append(new String(Arrays.copyOfRange(ref, alignmentStart-1 + span
						- shoulder, alignmentStart + span)));

				log.info(String
						.format("Slice md5 %s does not match calculated %s, %d:%d-%d, %s",
								sliceMD5, md5, sequenceId, alignmentStart,
								span, sb.toString()));
				return false;
			}
			return true;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeEOFException(e);
		}
	}

	public void setRefMD5(byte[] ref) {
		if (ref == null || ref.length == 0 || alignmentStart < 1) {
			refMD5 = new byte[16];
			Arrays.fill(refMD5, (byte) 0);

			log.info("Empty slice ref md5 is set.");
		} else {

			MessageDigest md5_MessageDigest = null;
			try {
				md5_MessageDigest = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeEOFException(e);
			}

			md5_MessageDigest.reset();

			int span = Math.min(alignmentSpan, ref.length - alignmentStart + 1);
//			System.out.println(new String (ref, alignmentStart-1, span));

			if (alignmentStart + alignmentSpan > ref.length + 1)
				throw new RuntimeException("Invalid alignment boundaries.");

			md5_MessageDigest.update(ref, alignmentStart - 1, span);

			refMD5 = md5_MessageDigest.digest();
			try {
				String md5 = Utils.calculateMD5(ref, alignmentStart-1, span) ;
				if (!md5.equals(String.format("%032x", new BigInteger(1, refMD5)))) {
					System.out.println("gotcha");
				}
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			StringBuffer sb = new StringBuffer();
			int shoulder = 10;
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart-1,
					alignmentStart + shoulder)));
			sb.append("...");
			sb.append(new String(Arrays.copyOfRange(ref, alignmentStart-1 + span
					- shoulder, alignmentStart + span)));

			log.info(String.format("Slice md5: %s for %d:%d-%d, %s",
					String.format("%032x", new BigInteger(1, refMD5)),
					sequenceId, alignmentStart, span, sb.toString()));
		}

		// String sliceRef = new String(ref, alignmentStart - 1,
		// Math.min(span, 30));
		// log.debug("Slice ref starts with: " + sliceRef);
		// log.debug("Slice ref md5: "
		// + (String.format("%032x", new BigInteger(1,s.refMD5))));
	}
}