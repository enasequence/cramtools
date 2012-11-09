package net.sf.cram.structure;

import java.util.List;
import java.util.Map;

import net.sf.cram.EncodingKey;
import net.sf.cram.EncodingParams;

public class CompressionHeader {
	public long firstRecordPosition ;
	
	public boolean mappedQualityScoreIncluded;
	public boolean unmappedQualityScoreIncluded;
	public boolean unmappedPlacedQualityScoreIncluded;
	public boolean readNamesIncluded;
	
	public Map<EncodingKey, EncodingParams> eMap;
	public Map<Integer, EncodingParams> tMap;
	public int[] landmarks ;

	public byte[][] substitutionCodes = new byte[256][256] ;

	public List<Integer> externalIds ;

}
