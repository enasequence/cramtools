package htsjdk.samtools.cram;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.cram.build.ContainerFactory;
import htsjdk.samtools.cram.build.Sam2CramRecordFactory;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.ref.ReferenceTracks;

import java.util.HashMap;
import java.util.Map;

import net.sf.cram.ref.ReferenceSource;

/**
 * Conversion context: holds all info required for converting SAMRecords to CRAM
 * container.
 * 
 * @author vadim
 *
 */
public class CramContext {
	private static final int RECORDS_PER_SLICE = 10000;
	public SAMFileHeader samFileHeader;
	public ReferenceSource referenceSource;
	public Map<Integer, ReferenceTracks> tracks = new HashMap<Integer, ReferenceTracks>();
	public CramLossyOptions lossyOptions;
	public long recordIndex = 0;

	Sam2CramRecordFactory sam2cramFactory;
	ContainerFactory containerFactory;

	public CramContext(SAMFileHeader samFileHeader, ReferenceSource referenceSource, CramLossyOptions lossyOptions) {
		this.samFileHeader = samFileHeader;
		this.referenceSource = referenceSource;
		this.lossyOptions = lossyOptions;

		sam2cramFactory = new Sam2CramRecordFactory(null, samFileHeader, CramVersions.CRAM_v3);
		containerFactory = new ContainerFactory(samFileHeader, RECORDS_PER_SLICE);
	}
}
