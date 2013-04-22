package net.sf.cram.structure;

import net.sf.samtools.SAMFileHeader;

public final class CramHeader {

	public static final byte[] magick = "CRAM".getBytes();
	public byte majorVersion;
	public byte minorVersion;
	public final byte[] id = new byte[20];

	public SAMFileHeader samFileHeader;

	public CramHeader() {
	}

	public CramHeader(int majorVersion, int minorVersion, String id,
			SAMFileHeader samFileHeader) {
		this.majorVersion = (byte) majorVersion;
		this.minorVersion = (byte) minorVersion;
		System.arraycopy(id.getBytes(), 0, this.id, 0,
				Math.min(id.length(), this.id.length));
		this.samFileHeader = samFileHeader;
	}

}