package net.sf.cram.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingID;
import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;
import net.sf.cram.ReadTag;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.structure.CompressionHeader;

public class DataReaderFactory {

	private boolean collectStats = false;

	public Reader buildReader(BitInputStream bis,
			Map<Integer, InputStream> inputMap, CompressionHeader h)
			throws IllegalArgumentException, IllegalAccessException {
		Reader reader = new Reader();
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
					Map<Integer, DataReader<byte[]>> map = new HashMap<Integer, DataReader<byte[]>>();
					for (Integer key : h.tMap.keySet()) {
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

		reader.tagIdDictionary = h.dictionary;
		return reader;
	}

	private <T> DataReader<T> createReader(DataSeriesType valueType,
			EncodingParams params, BitInputStream bis,
			Map<Integer, InputStream> inputMap) {
		if (params.id == EncodingID.NULL)
			return collectStats ? new DataReaderWithStats(
					buildNullReader(valueType)) : buildNullReader(valueType);

		EncodingFactory f = new EncodingFactory();
		Encoding<T> encoding = f.createEncoding(valueType, params.id);
		if (encoding == null)
			throw new RuntimeException("Encoding not found for value type "
					+ valueType.name() + ", id=" + params.id);
		encoding.fromByteArray(params.params);

		return collectStats ? new DataReaderWithStats(new DefaultDataReader<T>(
				encoding.buildCodec(inputMap, null), bis))
				: new DefaultDataReader<T>(encoding.buildCodec(inputMap, null),
						bis);
	}

	private static <T> DataReader<T> buildNullReader(DataSeriesType valueType) {
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
			return (DataReader<T>) new SingleValueReader<byte[]>(new byte[] {});

		default:
			throw new RuntimeException("Unknown data type: " + valueType.name());
		}
	}

	private static class DefaultDataReader<T> implements DataReader<T> {
		private BitCodec<T> codec;
		private BitInputStream bis;

		public DefaultDataReader(BitCodec<T> codec, BitInputStream bis) {
			this.codec = codec;
			this.bis = bis;
		}

		@Override
		public T readData() throws IOException {
			return codec.read(bis);
		}

		@Override
		public T readDataArray(int len) throws IOException {
			return codec.read(bis, len);
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

	public static class DataReaderWithStats<T> implements DataReader<T> {
		public long nanos = 0;
		DataReader<T> delegate;

		public DataReaderWithStats(DataReader<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public T readData() throws IOException {
			long time = System.nanoTime();
			T value = delegate.readData();
			nanos += System.nanoTime() - time;
			return value;
		}

		@Override
		public T readDataArray(int len) throws IOException {
			long time = System.nanoTime();
			T value = delegate.readDataArray(len);
			nanos += System.nanoTime() - time;
			return value;
		}
	}

	public Map<String, DataReaderWithStats> getStats(Reader reader)
			throws IllegalArgumentException, IllegalAccessException {
		Map<String, DataReaderWithStats> map = new TreeMap<String, DataReaderFactory.DataReaderWithStats>();
		if (!collectStats) return map ;

		for (Field f : reader.getClass().getFields()) {
			if (f.isAnnotationPresent(DataSeries.class)) {
				DataSeries ds = f.getAnnotation(DataSeries.class);
				EncodingKey key = ds.key();
				DataSeriesType type = ds.type();
				map.put(key.name(), (DataReaderWithStats) f.get(reader));
			}

			if (f.isAnnotationPresent(DataSeriesMap.class)) {
				DataSeriesMap dsm = f.getAnnotation(DataSeriesMap.class);
				String name = dsm.name();
				if ("TAG".equals(name)) {
					Map<Integer, DataReader<byte[]>> tagMap = (Map<Integer, DataReader<byte[]>>) f
							.get(reader);
					for (Integer key : tagMap.keySet()) {
						String tag = ReadTag.intToNameType4Bytes(key);
						map.put(tag, (DataReaderWithStats) tagMap.get(key));
					}
				}
			}
		}

		return map;
	}
}
