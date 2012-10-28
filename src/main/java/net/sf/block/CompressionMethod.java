package net.sf.block;

public enum CompressionMethod {
	RAW, GZIP;

	public byte byteValue() {
		return (byte) ordinal();
	}

	public static CompressionMethod fromByte(byte value) {
		return CompressionMethod.values()[0xFF & value];
	}
}
