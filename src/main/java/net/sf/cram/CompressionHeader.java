package net.sf.cram;

import java.util.List;
import java.util.Map;

public class CompressionHeader {
	public long firstRecordPosition ;
	
	public boolean mappedQualityScoreIncluded;
	public boolean unmappedQualityScoreIncluded;
	public boolean unmappedPlacedQualityScoreIncluded;
	public boolean readNamesIncluded;
	
	public Map<EncodingKey, EncodingParams> eMap;
	public Map<String, EncodingParams> tMap;
	public int[] landmarks ;

	public byte[][] substitutionCodes = new byte[256][256] ;

	public List<Integer> externalIds ;

}
