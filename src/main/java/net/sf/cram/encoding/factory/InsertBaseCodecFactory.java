package net.sf.cram.encoding.factory;

import java.io.IOException;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.read_features.InsertBase;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

class InsertBaseCodecFactory extends AbstractCodecFactory<InsertBase> {

	public InsertBaseCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<InsertBase> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {

		final BitCodec<Byte> baseCodec = bitCodecFactory
				.buildByteCodec(compression.eMap.get(EncodingKey.BA_Base));

		BitCodec<InsertBase> codec = new BitCodec<InsertBase>() {

			@Override
			public InsertBase read(BitInputStream bis) throws IOException {
				InsertBase ib = new InsertBase();
				ib.setBase(baseCodec.read(bis));
				return ib;
			}

			@Override
			public long write(BitOutputStream bos, InsertBase ib)
					throws IOException {
				return baseCodec.write(bos, ib.getBase());
			}

			@Override
			public long numberOfBits(InsertBase ib) {
				return baseCodec.numberOfBits(ib.getBase());
			}
		};

		DefaultMutableTreeNode node = buildNode(codec, "Single insertion codec");
		parent.add(node);

		return codec;
	}
}
