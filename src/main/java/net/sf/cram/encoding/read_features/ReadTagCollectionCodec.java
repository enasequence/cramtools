package net.sf.cram.encoding.read_features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.ReadTag;
import net.sf.cram.encoding.BitCodec;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.NullBitOutputStream;

public class ReadTagCollectionCodec implements BitCodec<Collection<ReadTag>> {
	public Map<String, BitCodec<byte[]>> valueCodecMap = new TreeMap<String, BitCodec<byte[]>>();
	public BitCodec<String> tagKeyCodec;

	@Override
	public Collection<ReadTag> read(BitInputStream bis) throws IOException {
		Collection<ReadTag> tags = null;
		while (bis.readBit()) {
			if (tags == null)
				tags = new ArrayList<ReadTag>();
			String tagKeyAndType = tagKeyCodec.read(bis);
			BitCodec<byte[]> codec = valueCodecMap.get(tagKeyAndType);
			byte[] valueBytes = codec.read(bis);
			char type = tagKeyAndType.charAt(3);
			Object value = ReadTag.restoreValueFromByteArray(type, valueBytes);

			ReadTag tag = new ReadTag(tagKeyAndType.substring(0, 2), type, value);
			tags.add(tag);
		}
		return tags;
	}

	@Override
	public long write(BitOutputStream bos, Collection<ReadTag> tags) throws IOException {
		long len = 0;
		if (tags != null && !tags.isEmpty()) {
			for (ReadTag tag : tags) {
				bos.write(true);
				len++;
				tagKeyCodec.write(bos, tag.getKeyAndType());
				BitCodec<byte[]> codec = valueCodecMap.get(tag.getKeyAndType());
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