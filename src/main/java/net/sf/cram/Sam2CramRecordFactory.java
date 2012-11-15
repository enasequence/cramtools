/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.sf.cram.encoding.read_features.BaseChange;
import net.sf.cram.encoding.read_features.BaseQualityScore;
import net.sf.cram.encoding.read_features.DeletionVariation;
import net.sf.cram.encoding.read_features.InsertBase;
import net.sf.cram.encoding.read_features.ReadFeature;
import net.sf.cram.encoding.read_features.SoftClipVariation;
import net.sf.cram.encoding.read_features.SubstitutionVariation;
import net.sf.cram.mask.RefMaskUtils;
import net.sf.picard.util.Log;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecord.SAMTagAndValue;
import net.sf.samtools.SAMTag;

public class Sam2CramRecordFactory {
	private enum TREAT_TYPE {
		IGNORE, ALIGNMENT, INSERTION, SOFT_CLIP
	};

	/**
	 * Reserved for later use.
	 */
	private TREAT_TYPE treatSoftClipsAs = TREAT_TYPE.SOFT_CLIP;

	public final static byte QS_asciiOffset = 33;
	public final static byte unsetQualityScore = 32;
	public final static byte ignorePositionsWithQualityScore = -1;

	private byte[] refBases;
	private byte[] refSNPs;
	private RefMaskUtils.RefMask refPile;

	public boolean captureUnmappedBases = true;
	public boolean captureUnmappedScores = false;

	private ByteBuffer insertionBuf = ByteBuffer.allocate(1024);

	private static Log log = Log.getInstance(Sam2CramRecordFactory.class);

	private Map<String, Integer> readGroupMap;

	private long landedRefMaskScores = 0;
	private long landedPiledScores = 0;
	private long landedTotalScores = 0;

	private boolean captureInsertScores = false;
	private boolean captureSubtitutionScores = false;
	private boolean captureFlankingDeletionScores = false;
	private int uncategorisedQualityScoreCutoff = 0;
	public boolean captureAllTags = false;
	public boolean preserveReadNames = false;
	public Set<String> captureTags = new TreeSet<String>();
	public Set<String> ignoreTags = new TreeSet<String>();
	{
		ignoreTags.add(SAMTag.NM.name());
		ignoreTags.add(SAMTag.MD.name());
		ignoreTags.add(SAMTag.RG.name());
	}

	public boolean losslessQS = false;

	public Sam2CramRecordFactory(byte[] refBases) {
		this(refBases, null, null, null);
	}

	public Sam2CramRecordFactory(byte[] refBases, byte[] refSNPs,
			RefMaskUtils.RefMask refPile, Map<String, Integer> readGroupMap) {
		this.refPile = refPile;
		// if (refBases == null)
		// throw new NullPointerException("Reference bases array is null.");
		this.refBases = refBases;
		this.refSNPs = refSNPs;
		this.readGroupMap = readGroupMap;
	}

	public Sam2CramRecordFactory() {
	}

	public CramRecord createCramRecord(SAMRecord record) {
		CramRecord cramRecord = new CramRecord();
		if (record.getReadPairedFlag()) {
			cramRecord.mateAlignmentStart = record.getMateAlignmentStart();
			cramRecord.mateUmapped = record.getMateUnmappedFlag();
			cramRecord.mateNegativeStrand = record.getMateNegativeStrandFlag();
			cramRecord.mateSequnceID = record.getMateReferenceIndex();
		}
		cramRecord.sequenceId = record.getReferenceIndex();
		cramRecord.setReadName(record.getReadName());
		cramRecord.setAlignmentStart(record.getAlignmentStart());

		cramRecord.multiFragment = record.getReadPairedFlag();
		cramRecord.properPair = record.getReadPairedFlag()
				&& record.getProperPairFlag();
		cramRecord.segmentUnmapped = record.getReadUnmappedFlag();
		cramRecord.negativeStrand = record.getReadNegativeStrandFlag();
		cramRecord.firstSegment = record.getReadPairedFlag()
				&& record.getFirstOfPairFlag();
		cramRecord.lastSegment = record.getReadPairedFlag()
				&& record.getSecondOfPairFlag();
		cramRecord.secondaryALignment = record.getNotPrimaryAlignmentFlag();
		cramRecord.vendorFiltered = record.getReadFailsVendorQualityCheckFlag();
		cramRecord.duplicate = record.getDuplicateReadFlag();

		cramRecord.setReadLength(record.getReadLength());
		cramRecord.setMappingQuality(record.getMappingQuality());
		cramRecord.setDuplicate(record.getDuplicateReadFlag());

		cramRecord.templateSize = record.getInferredInsertSize();
		if (readGroupMap != null) {
			SAMReadGroupRecord readGroup = record.getReadGroup();
			if (readGroup != null) {
				Integer rgIndex = readGroupMap.get(readGroup.getId());
				if (rgIndex == null)
					throw new RuntimeException("Read group index not found: "
							+ readGroup.getId());
				cramRecord.setReadGroupID(rgIndex);
			}
		}
		if (!record.getReadPairedFlag())
			cramRecord.setLastFragment(false);
		else {
			if (record.getFirstOfPairFlag())
				cramRecord.setLastFragment(false);
			else if (record.getSecondOfPairFlag())
				cramRecord.setLastFragment(true);
			else
				cramRecord.setLastFragment(true);
		}

		if (!cramRecord.segmentUnmapped) {
			List<ReadFeature> features = checkedCreateVariations(cramRecord,
					record);
			cramRecord.setReadFeatures(features);
		}

		cramRecord.setReadBases(record.getReadBases());
		cramRecord.setQualityScores(record.getBaseQualities());
		// for (int i = 0; i < cramRecord.getQualityScores().length; i++)
		// cramRecord.getQualityScores()[i] += QS_asciiOffset;
		landedTotalScores += cramRecord.getReadLength();

		if (captureAllTags) {
			List<SAMTagAndValue> attributes = record.getAttributes();
			if (attributes != null && !attributes.isEmpty()) {
				List<ReadTag> tags = new ArrayList<ReadTag>(attributes.size());
				for (SAMTagAndValue tv : attributes) {
					if (ignoreTags.contains(tv.tag))
						continue;

					ReadTag ra = ReadTag.deriveTypeFromValue(tv.tag, tv.value);
					tags.add(ra);
				}
				cramRecord.tags = tags;
			}
		} else {
			if (!captureTags.isEmpty()) {
				List<SAMTagAndValue> attributes = record.getAttributes();
				if (attributes != null && !attributes.isEmpty()) {
					List<ReadTag> tags = new ArrayList<ReadTag>(
							attributes.size());
					for (SAMTagAndValue tv : attributes) {
						if (captureTags.contains(tv.tag)) {
							ReadTag ra = ReadTag.deriveTypeFromValue(tv.tag,
									tv.value);
							tags.add(ra);
						}
					}
					cramRecord.tags = tags;
				}
			}
		}

		cramRecord.vendorFiltered = record.getReadFailsVendorQualityCheckFlag();

		if (preserveReadNames)
			cramRecord.setReadName(record.getReadName());

		return cramRecord;
	}

	/**
	 * A wrapper method to provide better diagnostics for
	 * ArrayIndexOutOfBoundsException.
	 * 
	 * @param cramRecord
	 * @param samRecord
	 * @return
	 */
	private List<ReadFeature> checkedCreateVariations(CramRecord cramRecord,
			SAMRecord samRecord) {
		try {
			return createVariations(cramRecord, samRecord);
		} catch (ArrayIndexOutOfBoundsException e) {
			log.error("Reference bases array length=" + refBases.length);
			log.error("Offensive CRAM record: " + cramRecord.toString());
			log.error("Offensive SAM record: " + samRecord.getSAMString());
			throw e;
		}
	}

	private List<ReadFeature> createVariations(CramRecord cramRecord,
			SAMRecord samRecord) {
		List<ReadFeature> features = new LinkedList<ReadFeature>();
		int zeroBasedPositionInRead = 0;
		int alignmentStartOffset = 0;
		int cigarElementLength = 0;

		List<CigarElement> cigarElements = samRecord.getCigar()
				.getCigarElements();

		byte[] bases = samRecord.getReadBases();
		byte[] qualityScore = samRecord.getBaseQualities();

		for (CigarElement cigarElement : cigarElements) {
			cigarElementLength = cigarElement.getLength();
			CigarOperator operator = cigarElement.getOperator();

			switch (operator) {
			case D:
			case N:
				features.add(new DeletionVariation(zeroBasedPositionInRead + 1,
						cigarElementLength));
				break;
			case H:
				break;
			case S:
				switch (treatSoftClipsAs) {
				// case ALIGNMENT:
				// addSubstitutionsAndMaskedBases(cramRecord, features,
				// zeroBasedPositionInRead, alignmentStartOffset,
				// cigarElementLength, bases, qualityScore);
				// break;
				case IGNORE:
					// zeroBasedPositionInRead += cigarElementLength;
					break;
				case INSERTION:
					addInsertion(features, zeroBasedPositionInRead,
							cigarElementLength, bases, qualityScore);
					break;
				case SOFT_CLIP:
					addSoftClip(features, zeroBasedPositionInRead,
							cigarElementLength, bases, qualityScore);
					break;
				default:
					throw new IllegalArgumentException(
							"Not sure how to treat soft clips: "
									+ treatSoftClipsAs);
				}
				break;
			case I:
				addInsertion(features, zeroBasedPositionInRead,
						cigarElementLength, bases, qualityScore);
				break;
			case M:
			case X:
			case EQ:
				addSubstitutionsAndMaskedBases(cramRecord, features,
						zeroBasedPositionInRead, alignmentStartOffset,
						cigarElementLength, bases, qualityScore);
				break;
			default:
				throw new IllegalArgumentException(
						"Unsupported cigar operator: "
								+ cigarElement.getOperator());
			}

			if (cigarElement.getOperator().consumesReadBases())
				zeroBasedPositionInRead += cigarElementLength;
			if (cigarElement.getOperator().consumesReferenceBases())
				alignmentStartOffset += cigarElementLength;
		}

		return features;
	}

	private void addSoftClip(List<ReadFeature> features,
			int zeroBasedPositionInRead, int cigarElementLength, byte[] bases,
			byte[] scores) {
		byte[] insertedBases = Arrays.copyOfRange(bases,
				zeroBasedPositionInRead, zeroBasedPositionInRead
						+ cigarElementLength);

		SoftClipVariation v = new SoftClipVariation(
				zeroBasedPositionInRead + 1, insertedBases);
		features.add(v);
	}

	private void addInsertion(List<ReadFeature> features,
			int zeroBasedPositionInRead, int cigarElementLength, byte[] bases,
			byte[] scores) {
		byte[] insertedBases = Arrays.copyOfRange(bases,
				zeroBasedPositionInRead, zeroBasedPositionInRead
						+ cigarElementLength);

		for (int i = 0; i < insertedBases.length; i++) {
			// single base insertion:
			InsertBase ib = new InsertBase();
			ib.setPosition(zeroBasedPositionInRead + 1 + i);
			ib.setBase(insertedBases[i]);
			features.add(ib);
			if (losslessQS)
				continue;
			boolean qualityMasked = (scores[i] < uncategorisedQualityScoreCutoff);
			if (captureInsertScores || qualityMasked) {
				byte score = (byte) (QS_asciiOffset + scores[zeroBasedPositionInRead
						+ i]);
				// if (score >= QS_asciiOffset) {
				features.add(new BaseQualityScore(zeroBasedPositionInRead + 1
						+ i, score));
				landedTotalScores++;
				// }
			}
		}
	}

	private void addSubstitutionsAndMaskedBases(CramRecord cramRecord,
			List<ReadFeature> features, int fromPosInRead,
			int alignmentStartOffset, int nofReadBases, byte[] bases,
			byte[] qualityScore) {
		int oneBasedPositionInRead;
		boolean noQS = (qualityScore.length == 0);

		int i = 0;
		boolean qualityAdded = false;
		boolean qualityMasked = false;
		byte refBase;
		for (i = 0; i < nofReadBases; i++) {
			oneBasedPositionInRead = i + fromPosInRead + 1;
			int refCoord = (int) (cramRecord.getAlignmentStart() + i + alignmentStartOffset) - 1;
			qualityAdded = false;
			if (refCoord >= refBases.length)
				refBase = 'N';
			else
				refBase = refBases[refCoord];

			if (bases[i + fromPosInRead] != refBase) {
				SubstitutionVariation sv = new SubstitutionVariation();
				sv.setPosition(oneBasedPositionInRead);
				sv.setBase(bases[i + fromPosInRead]);
				sv.setRefernceBase(refBase);
				sv.setBaseChange(new BaseChange(sv.getRefernceBase(), sv
						.getBase()));

				features.add(sv);

				if (losslessQS || noQS)
					continue;

				if (captureSubtitutionScores) {
					byte score = (byte) (QS_asciiOffset + qualityScore[i
							+ fromPosInRead]);
					features.add(new BaseQualityScore(oneBasedPositionInRead,
							score));
					qualityAdded = true;
				}
			}

			if (noQS)
				continue;

			if (!qualityAdded && refSNPs != null) {
				byte snpOrNot = refSNPs[refCoord];
				if (snpOrNot != 0) {
					byte score = (byte) (QS_asciiOffset + qualityScore[i
							+ fromPosInRead]);
					features.add(new BaseQualityScore(oneBasedPositionInRead,
							score));
					qualityAdded = true;
					landedRefMaskScores++;
				}
			}

			if (!qualityAdded && refPile != null) {
				if (refPile.shouldStore(refCoord, refBase)) {
					byte score = (byte) (QS_asciiOffset + qualityScore[i
							+ fromPosInRead]);
					features.add(new BaseQualityScore(oneBasedPositionInRead,
							score));
					qualityAdded = true;
					landedPiledScores++;
				}
			}

			qualityMasked = (qualityScore[i + fromPosInRead] < uncategorisedQualityScoreCutoff);
			if (!qualityAdded && qualityMasked) {
				byte score = (byte) (QS_asciiOffset + qualityScore[i
						+ fromPosInRead]);
				features.add(new BaseQualityScore(oneBasedPositionInRead, score));
				qualityAdded = true;
			}

			if (qualityAdded)
				landedTotalScores++;
		}
	}

	public boolean isCaptureInsertScores() {
		return captureInsertScores;
	}

	public void setCaptureInsertScores(boolean captureInsertScores) {
		this.captureInsertScores = captureInsertScores;
	}

	public boolean isCaptureSubtitutionScores() {
		return captureSubtitutionScores;
	}

	public void setCaptureSubtitutionScores(boolean captureSubtitutionScores) {
		this.captureSubtitutionScores = captureSubtitutionScores;
	}

	public int getUncategorisedQualityScoreCutoff() {
		return uncategorisedQualityScoreCutoff;
	}

	public void setUncategorisedQualityScoreCutoff(
			int uncategorisedQualityScoreCutoff) {
		this.uncategorisedQualityScoreCutoff = uncategorisedQualityScoreCutoff;
	}

	public long getLandedRefMaskScores() {
		return landedRefMaskScores;
	}

	public long getLandedPiledScores() {
		return landedPiledScores;
	}

	public long getLandedTotalScores() {
		return landedTotalScores;
	}

	public boolean isCaptureUnmappedBases() {
		return captureUnmappedBases;
	}

	public void setCaptureUnmappedBases(boolean captureUnmappedBases) {
		this.captureUnmappedBases = captureUnmappedBases;
	}

	public boolean isCaptureUnmappedScores() {
		return captureUnmappedScores;
	}

	public void setCaptureUnmappedScores(boolean captureUnmappedScores) {
		this.captureUnmappedScores = captureUnmappedScores;
	}

	public byte[] getRefBases() {
		return refBases;
	}

	public void setRefBases(byte[] refBases) {
		this.refBases = refBases;
	}

	public byte[] getRefSNPs() {
		return refSNPs;
	}

	public void setRefSNPs(byte[] refSNPs) {
		this.refSNPs = refSNPs;
	}

	public RefMaskUtils.RefMask getRefPile() {
		return refPile;
	}

	public Map<String, Integer> getReadGroupMap() {
		return readGroupMap;
	}

	public void setRefPile(RefMaskUtils.RefMask refPile) {
		this.refPile = refPile;
	}

}
