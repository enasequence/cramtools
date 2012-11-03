package net.sf.cram.encoding;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;
import net.sf.cram.io.BitOutputStream;
import net.sf.cram.io.ExposedByteArrayOutputStream;
import net.sf.cram.structure.CompressionHeader;

public class DataWriterFactory {

	public Writer buildWriter(BitOutputStream bos,
			Map<Integer, ExposedByteArrayOutputStream> outputMap,
			CompressionHeader h) throws IllegalArgumentException,
			IllegalAccessException {
		Writer writer = new Writer();
		writer.captureMappedQS = h.mappedQualityScoreIncluded;
		writer.captureUnmappedQS = h.unmappedQualityScoreIncluded;
		writer.captureReadNames = h.readNamesIncluded;

		for (Field f : writer.getClass().getFields()) {
			if (f.isAnnotationPresent(DataSeries.class)) {
				DataSeries ds = f.getAnnotation(DataSeries.class);
				EncodingKey key = ds.key();
				DataSeriesType type = ds.type();
				f.set(writer,
						createWriter(type, h.eMap.get(key), bos, outputMap));
			}

			if (f.isAnnotationPresent(DataSeriesMap.class)) {
				DataSeriesMap dsm = f.getAnnotation(DataSeriesMap.class);
				String name = dsm.name();
				if ("TAG".equals(name)) {
					Map<String, DataWriter<byte[]>> map = new HashMap<String, DataWriter<byte[]>>();
					for (String key : h.tMap.keySet()) {
						EncodingParams params = h.tMap.get(key);
						DataWriter<byte[]> tagWtiter = createWriter(
								DataSeriesType.BYTE_ARRAY, params, bos,
								outputMap);
						map.put(key, tagWtiter);
					}
					f.set(writer, map);
				}
			}
		}

		return writer;
	}

	private <T> DataWriter<T> createWriter(DataSeriesType valueType,
			EncodingParams params, BitOutputStream bos,
			Map<Integer, ExposedByteArrayOutputStream> outputMap) {
		EncodingFactory f = new EncodingFactory();
		Encoding<T> encoding = f.createEncoding(valueType, params.id);
		if (encoding == null)
			throw new RuntimeException("Encoding not found: value type="
					+ valueType.name() + ", encoding id=" + params.id.name());

		encoding.fromByteArray(params.params);

		return new DefaultDataWriter<T>(encoding.buildCodec(null, outputMap),
				bos);
	}

	private static class DefaultDataWriter<T> implements DataWriter<T> {
		private BitCodec<T> codec;
		private BitOutputStream bos;

		public DefaultDataWriter(BitCodec<T> codec, BitOutputStream bos) {
			this.codec = codec;
			this.bos = bos;
		}

		@Override
		public long writeData(T value) throws IOException {
			return codec.write(bos, value);
		}

	}
}
