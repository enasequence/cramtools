package net.sf.cram.encoding;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingID;

public class EncodingFactory {

	public <T> Encoding<T> createEncoding(DataSeriesType valueType,
			EncodingID id) {
		switch (valueType) {
		case BYTE:
			switch (id) {
			case EXTERNAL:
				return (Encoding<T>) new ExternalByteEncoding();
			case HUFFMAN:
				return (Encoding<T>) new HuffmanByteEncoding();
			case NULL:
				return new NullEncoding<>();

			default:
				break;
			}

			break;

		case INT:
			switch (id) {
			case HUFFMAN:
				return (Encoding<T>) new HuffmanIntegerEncoding();
			case NULL:
				return new NullEncoding<>();
			case EXTERNAL:
				return (Encoding<T>) new ExternalIntegerEncoding();
			case GOLOMB:
				return (Encoding<T>) new GolombIntegerEncoding();

			default:
				break;
			}
			break;

		case LONG:
			switch (id) {
			case NULL:
				return new NullEncoding<>();
			case GOLOMB:
				return (Encoding<T>) new GolombEncoding();
			case EXTERNAL:
				return (Encoding<T>) new ExternalLongEncoding();

			default:
				break;
			}
			break;

		case BYTE_ARRAY:
			switch (id) {
			case NULL:
				return new NullEncoding<>();
			case BYTE_ARRAY_LEN:
				return (Encoding<T>) new ByteArrayLenEncoding();
			case EXTERNAL:
				return (Encoding<T>) new ExternalByteArrayEncoding();

			default:
				break;
			}
			break;

		default:
			break;
		}

		return null;
	}

	public Encoding<byte[]> createByteArrayEncoding(EncodingID id) {
		switch (id) {
		case EXTERNAL:
			return new ByteArrayLenEncoding();

		default:
			break;
		}
		return null;
	}

	public Encoding<Byte> createByteEncoding(EncodingID id) {
		switch (id) {
		case EXTERNAL:
			return new ExternalByteEncoding();

		default:
			break;
		}
		return null;
	}

	public Encoding<Integer> createIntEncoding(EncodingID id) {
		return null;
	}
}
