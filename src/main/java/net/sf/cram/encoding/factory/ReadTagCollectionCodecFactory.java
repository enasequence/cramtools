package net.sf.cram.encoding.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.cram.CompressionHeader;
import net.sf.cram.EncodingKey;
import net.sf.cram.encoding.BitCodec;
import net.sf.cram.encoding.Encoding;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.ReadTag;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

class ReadTagCollectionCodecFactory extends
		AbstractCodecFactory<Collection<ReadTag>> {

	public ReadTagCollectionCodecFactory(boolean useMeasuringCodecWrapper,
			BitCodecFactory bitCodecFactory) {
		super(useMeasuringCodecWrapper, bitCodecFactory);
	}

	@Override
	public BitCodec<Collection<ReadTag>> buildCodec(CramHeader header,
			CompressionHeader compression,
			SequenceBaseProvider referenceBaseProvider,
			DefaultMutableTreeNode parent) throws CramCompressionException {

		Set<String> tagNameAndTypes = compression.tMap.keySet();
		BitCodec<Byte> tagKeyCodec = bitCodecFactory
				.buildByteCodec(compression.eMap
						.get(EncodingKey.TN_TagNameAndType));
		Map<String, Encoding<?>> tagStubs = compression.tMap;

		List<BitCodec<byte[]>> tagCodecs = new ArrayList<BitCodec<byte[]>>(
				tagNameAndTypes.size());
		ReadTag[] tags = new ReadTag[tagNameAndTypes.size()];
		byte i = 0;
		for (String nameAndType : tagNameAndTypes) {
			BitCodec<byte[]> tagCodec = bitCodecFactory
					.buildByteArrayCodec(tagStubs.get(nameAndType));
			tagCodecs.add(tagCodec);

			ReadTag tag = ReadTag.deriveTypeFromKeyAndType(nameAndType, null);
			tag.setIndex(i);
			tags[i] = tag;
			i++;
		}

		ReadTagCollectionCodec codec = new ReadTagCollectionCodec();
		codec.tagCodecs = tagCodecs;
		codec.tagKeyCodec = tagKeyCodec;
		codec.tags = tags;

		return register(codec, parent, "Read tag codec");

	};

	private class ReadTagCollectionCodec implements
			BitCodec<Collection<ReadTag>> {
		public ReadTag[] tags;
		public BitCodec<Byte> tagKeyCodec;
		public List<BitCodec<byte[]>> tagCodecs;

		@Override
		public Collection<ReadTag> read(BitInputStream bis) throws IOException {
			Collection<ReadTag> foundTags = null;
			while (bis.readBit()) {
				if (foundTags == null)
					foundTags = new ArrayList<ReadTag>();

				int index = 0xFF & tagKeyCodec.read(bis);
				ReadTag tag = tags[index];
				String tagKeyAndType = tag.getKeyAndType();
				BitCodec<byte[]> codec = tagCodecs.get(index);

				byte[] valueBytes = codec.read(bis);
				char type = tagKeyAndType.charAt(3);
				Object value = ReadTag.restoreValueFromByteArray(type,
						valueBytes);

				ReadTag foundTag = new ReadTag(tagKeyAndType.substring(0, 2),
						type, value);
				foundTags.add(foundTag);
			}
			return foundTags;
		}

		@Override
		public long write(BitOutputStream bos, Collection<ReadTag> tags)
				throws IOException {
			long len = 0;
			if (tags != null && !tags.isEmpty()) {
				for (ReadTag tag : tags) {
					bos.write(true);
					len++;
					tagKeyCodec.write(bos, tag.getIndex());
					BitCodec<byte[]> codec = tagCodecs.get(0xFF & tag
							.getIndex());
					long bits = codec.write(bos, tag.getValueAsByteArray());
					len += bits;
				}
			}
			bos.write(false);
			len++;
			return len;
		}

		@Override
		public long numberOfBits(Collection<ReadTag> tags) {
			try {
				return write(NullBitOutputStream.INSTANCE, tags);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
