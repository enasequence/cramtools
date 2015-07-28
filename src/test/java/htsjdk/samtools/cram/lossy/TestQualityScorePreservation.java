/*******************************************************************************
 * Copyright 2013 EMBL-EBI
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
package htsjdk.samtools.cram.lossy;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMUtils;
import htsjdk.samtools.cram.build.CramNormalizer;
import htsjdk.samtools.cram.build.Sam2CramRecordFactory;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.ref.ReferenceTracks;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import org.junit.Ignore;
import org.junit.Test;

public class TestQualityScorePreservation {

	@Test
	public void test1() {
		QualityScorePreservation p = new QualityScorePreservation("m999_8");
		List<PreservationPolicy> policies = p.getPreservationPolicies();

		assertNotNull(p);
		assertEquals(policies.size(), 1);

		PreservationPolicy policy0 = policies.get(0);
		assertThat(policy0.readCategory.type, is(ReadCategoryType.LOWER_MAPPING_SCORE));

		assertThat(policy0.readCategory.param, is(999));

		if (policy0.baseCategories != null)
			assertEquals(policy0.baseCategories.isEmpty(), true);

		QualityScoreTreatment treatment = policy0.treatment;
		assertNotNull(treatment);

		assertThat(treatment.type, is(QualityScoreTreatmentType.BIN));
		assertThat(treatment.param, is(8));
	}

	@Test
	public void test2() {
		QualityScorePreservation p = new QualityScorePreservation("R8-N40");
		List<PreservationPolicy> policies = p.getPreservationPolicies();

		assertNotNull(p);
		assertEquals(policies.size(), 2);

		{
			PreservationPolicy policy0 = policies.get(0);
			assertNull(policy0.readCategory);

			List<BaseCategory> baseCategories = policy0.baseCategories;
			assertNotNull(baseCategories);
			assertEquals(baseCategories.size(), 1);

			BaseCategory c0 = baseCategories.get(0);
			assertEquals(c0.type, BaseCategoryType.MATCH);
			assertEquals(c0.param, -1);

			QualityScoreTreatment treatment = policy0.treatment;
			assertNotNull(treatment);

			assertThat(treatment.type, is(QualityScoreTreatmentType.BIN));
			assertThat(treatment.param, is(8));
		}

		{
			PreservationPolicy policy1 = policies.get(1);
			assertNull(policy1.readCategory);

			List<BaseCategory> baseCategories = policy1.baseCategories;
			assertNotNull(baseCategories);
			assertEquals(baseCategories.size(), 1);

			BaseCategory c0 = baseCategories.get(0);
			assertEquals(c0.type, BaseCategoryType.MISMATCH);
			assertEquals(c0.param, -1);

			QualityScoreTreatment treatment = policy1.treatment;
			assertNotNull(treatment);

			assertThat(treatment.type, is(QualityScoreTreatmentType.PRESERVE));
			assertThat(treatment.param, is(40));
		}
	}

	private SAMFileHeader samFileHeader = new SAMFileHeader();

	private SAMRecord buildSAMRecord(String seqName, String line) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write("@HD\tVN:1.0\tGO:none SO:coordinate\n".getBytes());
			baos.write(("@SQ\tSN:" + seqName + "\tLN:247249719\n").getBytes());
			baos.write(line.replaceAll("\\s+", "\t").getBytes());
			baos.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		SAMFileReader r = new SAMFileReader(bais);
		try {
			return r.iterator().next();
		} finally {
			r.close();
		}
	}

	@Ignore("Broken test.")
	@Test
	public void test3() {
		String line1 = "98573 1107 20 1 60 100M = 999587 -415 CTGGTCTTAGTTCCGCAAGTGGGTATATATAAAGGCTCAAAATCAATCTTTATATTGACATCTCTCTACTTATTTGTGTTGTCTGATGCTCATATTGTAG ::A<<=D@BBC;C9=7DEEBHDEHHACEEBEEEDEE=EFFHEEFFFEHEF@HFBCEFEHFEHEHFEHDHHHFHHHEHHHHDFHHHHHGHHHHHHHHHHHH";
		String line2 = "98738 1187 20 18 29 99M1S = 1000253 432 AGCGGGGATATATAAAGGCTCAAAATTACTTTTTATATGGACAACTCTCTACTGCTTTGAGATGACTGATACTCATATTGATGGAGCTTTATCAAGAAAT !\"#$%&'()*+-./0'''''''''''#'#'#'''''''#''''#'''''''''##''''#'#''#'''''#'''''''''##''''#''##''''''''?";
		String seqName = "20";
		List<String> lines = Arrays.asList(new String[] { line2, line1 });

		byte[] ref = "CTGGTCTTAGTTCCGCAAGTGGGTATATATAAAGGCTCAAAATCAATCTTTATATTGACATCTCTCTACTTATTTGTGTTGTCTGATGCTCATATTGTAGGAGATTCCTCAAGAAAGG"
				.getBytes();
		ReferenceTracks tracks = new ReferenceTracks(0, seqName, ref);
		QualityScorePreservation p = new QualityScorePreservation("R8-N40-M40-D40");

		for (String line : lines) {
			SAMRecord record = buildSAMRecord(seqName, line);

			Sam2CramRecordFactory f = new Sam2CramRecordFactory(ref, record.getHeader(), CramVersions.CRAM_v3);
			CramCompressionRecord cramRecord = f.createCramRecord(record);

			p.addQualityScores(record, cramRecord, tracks);
			if (!cramRecord.isForcePreserveQualityScores()) {
				CramNormalizer.restoreQualityScores((byte) 30, Collections.singletonList(cramRecord));
			}

			StringBuffer sb = new StringBuffer();
			sb.append(record.getBaseQualityString());
			sb.append("\n");
			sb.append(SAMUtils.phredToFastq(cramRecord.qualityScores));

			assertArrayEquals(sb.toString(), record.getBaseQualities(), cramRecord.qualityScores);
		}
	}

	@Ignore("Broken test.")
	@Test
	public void test4() {
		String line2 = "98738 1187 20 18 29 99M1S = 1000253 432 AGCGGGGATATATAAAGGCTCAAAATTACTTTTTATATGGACAACTCTCTACTGCTTTGAGATGACTGATACTCATATTGATGGAGCTTTATCAAGAAAT !\"#$%&'()*+-./0'''''''''''#'#'#'''''''#''''#'''''''''##''''#'#''#'''''#'''''''''##''''#''##''''''''?";
		String seqName = "20";
		List<String> lines = Arrays.asList(new String[] { line2 });

		byte[] ref = "CTGGTCTTAGTTCCGCAAGTGGGTATATATAAAGGCTCAAAATCAATCTTTATATTGACATCTCTCTACTTATTTGTGTTGTCTGATGCTCATATTGTAGGAGATTCCTCAAGAAAGG"
				.getBytes();
		ReferenceTracks tracks = new ReferenceTracks(0, seqName, ref);
		QualityScorePreservation p = new QualityScorePreservation("R40X10-N40-U40");
		for (int i = 0; i < ref.length; i++)
			tracks.addCoverage(i + 1, 66);

		for (String line : lines) {
			SAMRecord record = buildSAMRecord(seqName, line);

			Sam2CramRecordFactory f = new Sam2CramRecordFactory(ref, record.getHeader(), CramVersions.CRAM_v3);
			CramCompressionRecord cramRecord = f.createCramRecord(record);

			p.addQualityScores(record, cramRecord, tracks);
			if (!cramRecord.isForcePreserveQualityScores()) {
				CramNormalizer.restoreQualityScores((byte) 30, Collections.singletonList(cramRecord));
			}

			StringBuffer sb = new StringBuffer();
			sb.append(record.getBaseQualityString());
			sb.append("\n");
			sb.append(SAMUtils.phredToFastq(cramRecord.qualityScores));

			assertArrayEquals(sb.toString(), record.getBaseQualities(), cramRecord.qualityScores);
		}
	}

}
