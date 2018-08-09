/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
		sam2cramFactory.captureAllTags = lossyOptions.isCaptureAllTags();
		sam2cramFactory.captureTags.addAll(lossyOptions.getCaptureTags());
		sam2cramFactory.ignoreTags.addAll(lossyOptions.getIgnoreTags());
		containerFactory = new ContainerFactory(samFileHeader, RECORDS_PER_SLICE);
	}
}
