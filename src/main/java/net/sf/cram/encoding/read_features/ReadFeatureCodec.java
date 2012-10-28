package net.sf.cram.encoding.read_features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.format.ReadFeature;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class ReadFeatureCodec implements BitCodec<List<ReadFeature>> {
	public BitCodec<Long> inReadPosCodec;
	public BitCodec<ReadBase> readBaseCodec;
	public BitCodec<SubstitutionVariation> substitutionCodec;
	public BitCodec<InsertionVariation> insertionCodec;
	public BitCodec<DeletionVariation> deletionCodec;
	public BitCodec<BaseQualityScore> baseQSCodec;
	public BitCodec<InsertBase> insertBaseCodec;

	public BitCodec<Byte> featureOperationCodec;

	private static Logger log = Logger.getLogger(ReadFeatureCodec.class.getName());

	@Override
	public List<ReadFeature> read(BitInputStream bis) throws IOException {
		List<ReadFeature> list = new ArrayList<ReadFeature>();
		byte op;
		int prevPos = 0;
		while ((op = featureOperationCodec.read(bis)) != ReadFeature.STOP_OPERATOR) {
			ReadFeature feature = null;
			int pos = prevPos + inReadPosCodec.read(bis).intValue();
			prevPos = pos;
			switch (op) {
			case ReadBase.operator:
				ReadBase readBase = readBaseCodec.read(bis);
				readBase.setPosition(pos);
				feature = readBase;
				break;
			case SubstitutionVariation.operator:
				SubstitutionVariation sub = substitutionCodec.read(bis);
				sub.setPosition(pos);
				feature = sub;
				break;
			case InsertionVariation.operator:
				InsertionVariation ins = insertionCodec.read(bis);
				ins.setPosition(pos);
				feature = ins;
				break;
			case DeletionVariation.operator:
				DeletionVariation del = deletionCodec.read(bis);
				del.setPosition(pos);
				feature = del;
				break;
			case InsertBase.operator:
				InsertBase ib = insertBaseCodec.read(bis);
				ib.setPosition(pos);
				feature = ib;
				break;
			case BaseQualityScore.operator:
				BaseQualityScore bqs = baseQSCodec.read(bis);
				bqs.setPosition(pos);
				feature = bqs;
				break;

			default:
				throw new RuntimeException("Unknown read feature operator: "
						+ (char) op);
			}
			list.add(feature);
		}

		return list;
	}

	@Override
	public long write(BitOutputStream bos, List<ReadFeature> features)
			throws IOException {

		long len = 0L;
		int prevPos = 0;
		for (ReadFeature feature : features) {
			len += featureOperationCodec.write(bos, feature.getOperator());

			len += inReadPosCodec.write(bos, (long) feature.getPosition()
					- prevPos);

			prevPos = feature.getPosition();
			switch (feature.getOperator()) {
			case ReadBase.operator:
				len += readBaseCodec.write(bos, (ReadBase) feature);
				break;
			case SubstitutionVariation.operator:

				len += substitutionCodec.write(bos,
						(SubstitutionVariation) feature);
				break;
			case InsertionVariation.operator:
				len += insertionCodec.write(bos, (InsertionVariation) feature);
				break;
			case DeletionVariation.operator:
				len += deletionCodec.write(bos, (DeletionVariation) feature);
				break;
			case InsertBase.operator:
				len += insertBaseCodec.write(bos, (InsertBase) feature);
				break;
			case BaseQualityScore.operator:
				len += baseQSCodec.write(bos, (BaseQualityScore) feature);
				break;

			default:
				throw new RuntimeException("Unknown read feature operator: "
						+ (char) feature.getOperator());
			}
		}
		len += featureOperationCodec.write(bos, ReadFeature.STOP_OPERATOR);

		return len;
	}

	@Override
	public long numberOfBits(List<ReadFeature> features) {
		try {
			return write(NullBitOutputStream.INSTANCE, features);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
