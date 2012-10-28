package net.sf.cram;

import java.util.Map;

public class CompressionHeader {
	public long firstRecordPosition ;
	
	public boolean mappedQualityScoreIncluded;
	
	public Map<EncodingKey, EncodingParams> eMap;
	public Map<String, EncodingParams> tMap;
	public int[] landmarks ;

	public byte[][] substitutionCodes = new byte[256][256] ;
}
