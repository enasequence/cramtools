package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.structure.CompressionHeader;

public class DataReaderFactory {

	public Reader buildReader(BitInputStream bis,
			Map<Integer, InputStream> inputMap, CompressionHeader h)
			throws IllegalArgumentException, IllegalAccessException {
		Reader reader = new Reader();
		reader.captureMappedQS = h.mappedQualityScoreIncluded;
		reader.captureUnmappedQS = h.unmappedQualityScoreIncluded;
		reader.captureReadNames = h.readNamesIncluded;

		for (Field f : reader.getClass().getFields()) {
			if (f.isAnnotationPresent(DataSeries.class)) {
				// debug hook:
				// if (f.getName().equals("fc"))
				// System.out.println("qwe");
				DataSeries ds = f.getAnnotation(DataSeries.class);
				EncodingKey key = ds.key();
				DataSeriesType type = ds.type();
				if (h.eMap.get(key) == null) {
					System.err.println("Encoding not found for key: " + key);
				}
				f.set(reader,
						createReader(type, h.eMap.get(key), bis, inputMap));
			}

			if (f.isAnnotationPresent(DataSeriesMap.class)) {
				DataSeriesMap dsm = f.getAnnotation(DataSeriesMap.class);
				String name = dsm.name();
				if ("TAG".equals(name)) {
					Map<String, DataReader<byte[]>> map = new HashMap<String, DataReader<byte[]>>();
					for (String key : h.tMap.keySet()) {
						EncodingParams params = h.tMap.get(key);
						DataReader<byte[]> tagReader = createReader(
								DataSeriesType.BYTE_ARRAY, params, bis,
								inputMap);
						map.put(key, tagReader);
					}
					f.set(reader, map);
				}
			}
		}

		return reader;
	}

	private <T> DataReader<T> createReader(DataSeriesType valueType,
			EncodingParams params, BitInputStream bis,
			Map<Integer, InputStream> inputMap) {
		if (params.id == EncodingID.NULL) {
			switch (valueType) {
			case BYTE:
				return (DataReader<T>) new SingleValueReader<Byte>(new Byte(
						(byte) 0));
			case INT:
				return (DataReader<T>) new SingleValueReader<Integer>(
						new Integer(0));
			case LONG:
				return (DataReader<T>) new SingleValueReader<Long>(new Long(0));
			case BYTE_ARRAY:
				return (DataReader<T>) new SingleValueReader<byte[]>(
						new byte[] {});

			default:
				break;
			}
		}

		EncodingFactory f = new EncodingFactory();
		Encoding<T> encoding = f.createEncoding(valueType, params.id);
		encoding.fromByteArray(params.params);

		return new DefaultDataReader<T>(encoding.buildCodec(inputMap, null),
				bis);
	}

	private static class DefaultDataReader<T> implements DataReader<T> {
		private BitCodec<T> codec;
		private BitInputStream bos;

		public DefaultDataReader(BitCodec<T> codec, BitInputStream bos) {
			this.codec = codec;
			this.bos = bos;
		}

		@Override
		public T readData() throws IOException {
			return codec.read(bos);
		}

		@Override
		public T readDataArray(int len) {
			// TODO Auto-generated method stub
			return null;
		}
	}

	private static class SingleValueReader<T> implements DataReader<T> {
		private T value;

		public SingleValueReader(T value) {
			super();
			this.value = value;
		}

		@Override
		public T readData() throws IOException {
			return value;
		}

		@Override
		public T readDataArray(int len) {
			return value;
		}

	}
}
