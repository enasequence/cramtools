package net.sf.cram;

public enum ContentType {
	HEADER, CORE, MAP_SLICE, UNMAPPED_SLICE;
	
	public static ContentType fromByte (byte value) {
		return ContentType.values()[0xFF & value] ;
	}
	
	public byte getContentType () {
		return (byte)ordinal() ;
	}
}
