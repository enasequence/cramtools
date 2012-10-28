package net.sf.cram;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.encoding.ByteArrayBitCodec;
import uk.ac.ebi.ena.sra.cram.format.ReadFeature;
import uk.ac.ebi.ena.sra.cram.format.ReadTag;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class CramRecordCodec implements BitCodec<CramRecord> {
	public BitCodec<Long> inSeqPosCodec;
	public BitCodec<Long> recordsToNextFragmentCodec;
	public BitCodec<Long> readlengthCodec;
	public BitCodec<List<ReadFeature>> variationsCodec;

	public BitCodec<byte[]> baseCodec;
	public ByteArrayBitCodec qualityCodec;

	public long prevPosInSeq = 1L;

	public BitCodec<Integer> readGroupCodec;

	public BitCodec<Byte> mappingQualityCodec;

	public boolean storeMappedQualityScores = false;

	public BitCodec<Collection<ReadTag>> readTagCodec;

	public BitCodec<Byte> flagsCodec;

	private static Logger log = Logger.getLogger(CramRecordCodec.class
			.getName());

	private static int debugRecordEndMarkerLen = 0;
	private static long debugRecordEndMarker = ~(-1 << debugRecordEndMarkerLen);

	public BitCodec<String> sequenceNameCodec;
	public BitCodec<byte[]> readNameCodec;
	public BitCodec<Integer> insertSizeCodec;
	public BitCodec<Long> mateAlignemntStartCodec;

	public boolean readNamesIncluded = false;

	@Override
	public CramRecord read(BitInputStream bis) throws IOException {
		long marker = bis.readLongBits(debugRecordEndMarkerLen);
		if (marker != debugRecordEndMarker) {
			throw new RuntimeException(
					"Debug marker for beginning of record not found.");
		}

		CramRecord record = new CramRecord();

		byte b = flagsCodec.read(bis);
		record.setFlags(b);

		if (readNamesIncluded)
			record.setReadName(new String(readNameCodec.read(bis)));

		if (!record.isLastFragment()) {
			if (!record.detached) {
				record.setRecordsToNextFragment(recordsToNextFragmentCodec
						.read(bis));
			} else {
				CramRecord mate = new CramRecord();
				mate.setReadMapped(bis.readBit());
				mate.setNegativeStrand(bis.readBit());
				mate.setFirstInPair(bis.readBit());

				if (!readNamesIncluded)
					record.setReadName(new String(readNameCodec.read(bis)));

				mate.setReadName(record.getReadName());
				mate.setSequenceName(sequenceNameCodec.read(bis));
				mate.setAlignmentStart(mateAlignemntStartCodec.read(bis));
				record.templateSize = insertSizeCodec.read(bis);

				mate.setFirstInPair(!record.isFirstInPair());
				if (record.isFirstInPair())
					record.next = mate;
				else
					record.previous = mate;

			}
		}

		int readLen = readlengthCodec.read(bis).intValue();
		record.setReadLength(readLen);

		if (record.isReadMapped()) {
			long position = prevPosInSeq + inSeqPosCodec.read(bis);
			prevPosInSeq = position;
			record.setAlignmentStart(position);

			boolean imperfectMatch = bis.readBit();
			if (imperfectMatch) {
				List<ReadFeature> features = variationsCodec.read(bis);
				record.setReadFeatures(features);
			}

			if (storeMappedQualityScores) {
				byte[] scores = qualityCodec.read(bis, readLen);
				// byte[] scores = new byte[readLen];
				// readNonEmptyByteArray(bis, scores, qualityCodec);
				record.setQualityScores(scores);
			}

			record.setMappingQuality(mappingQualityCodec.read(bis));
		} else {
			long position = prevPosInSeq + inSeqPosCodec.read(bis);
			prevPosInSeq = position;
			record.setAlignmentStart(position);

			byte[] bases = baseCodec.read(bis);
			record.setReadBases(bases);

			byte[] scores = qualityCodec.read(bis, readLen);
			record.setQualityScores(scores);
		}

		record.setReadGroupID(readGroupCodec.read(bis).intValue());

		record.tags = readTagCodec.read(bis);

		marker = bis.readLongBits(debugRecordEndMarkerLen);
		if (marker != debugRecordEndMarker) {
			System.out.println(record.toString());
			throw new RuntimeException(
					"Debug marker for end of record not found.");
		}

		return record;
	}

	@Override
	public long write(BitOutputStream bos, CramRecord record)
			throws IOException {
		bos.write(debugRecordEndMarker, debugRecordEndMarkerLen);

		long len = 0L;

		len += flagsCodec.write(bos, record.getFlags());

		if (readNamesIncluded)
			readNameCodec.write(bos, record.getReadName().getBytes());

		if (!record.isLastFragment()) {
			if (record.getRecordsToNextFragment() > 0) {
				len += recordsToNextFragmentCodec.write(bos,
						record.getRecordsToNextFragment());
			} else {

				CramRecord mate = record.next == null ? record.previous
						: record.next;
				bos.write(mate.isReadMapped());
				bos.write(mate.isNegativeStrand());
				bos.write(mate.isFirstInPair());

				if (!readNamesIncluded)
					readNameCodec.write(bos, record.getReadName().getBytes());
				sequenceNameCodec.write(bos, mate.getSequenceName());
				mateAlignemntStartCodec.write(bos, mate.getAlignmentStart());
				insertSizeCodec.write(bos, record.templateSize);
			}
		}

		len += readlengthCodec.write(bos, record.getReadLength());

		if (record.isReadMapped()) {
			if (record.getAlignmentStart() - prevPosInSeq < 0) {
				log.severe("Negative relative position in sequence: prev="
						+ prevPosInSeq);
				log.severe(record.toString());
			}
			len += inSeqPosCodec.write(bos, record.getAlignmentStart()
					- prevPosInSeq);
			prevPosInSeq = record.getAlignmentStart();

			List<ReadFeature> vars = record.getReadFeatures();
			if (vars == null || vars.isEmpty())
				bos.write(false);
			else {
				bos.write(true);
				len += variationsCodec.write(bos, vars);
			}
			len++;

			if (storeMappedQualityScores)
				// len += writeNonEmptyByteArray(bos, record.getQualityScores(),
				// qualityCodec);
				len += qualityCodec.write(bos, record.getQualityScores());

			mappingQualityCodec.write(bos, record.getMappingQuality());
		} else {
			if (record.getAlignmentStart() - prevPosInSeq < 0) {
				log.severe("Negative relative position in sequence: prev="
						+ prevPosInSeq);
				log.severe(record.toString());
			}
			len += inSeqPosCodec.write(bos, record.getAlignmentStart()
					- prevPosInSeq);
			prevPosInSeq = record.getAlignmentStart();

			len += baseCodec.write(bos, record.getReadBases());
			len += qualityCodec.write(bos, record.getQualityScores());
		}

		len += readGroupCodec.write(bos, record.getReadGroupID());

		len += readTagCodec.write(bos, record.tags);

		bos.write(debugRecordEndMarker, debugRecordEndMarkerLen);

		return len;
	}

	@Override
	public long numberOfBits(CramRecord record) {
		try {
			return write(NullBitOutputStream.INSTANCE, record);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
