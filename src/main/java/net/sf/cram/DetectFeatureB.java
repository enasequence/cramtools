package net.sf.cram;

import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.encoding.reader.CramRecordReader;
import htsjdk.samtools.cram.encoding.reader.DataReaderFactory;
import htsjdk.samtools.cram.encoding.readfeatures.ReadBase;
import htsjdk.samtools.cram.encoding.readfeatures.ReadFeature;
import htsjdk.samtools.cram.io.DefaultBitInputStream;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class DetectFeatureB {

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);
		File cramFile = new File(args[0]);
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		CramHeader header = CramIO.readCramHeader(is);
		Container c = null;
		while ((c = ContainerIO.readContainer(header.getVersion(), is)) != null && !c.isEOF()) {
			for (Slice slice : c.slices) {
				final DataReaderFactory dataReaderFactory = new DataReaderFactory();
				final Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
				for (final Integer exId : slice.external.keySet()) {
					inputMap.put(exId, new ByteArrayInputStream(slice.external.get(exId).getRawContent()));
				}

				final CramRecordReader reader = new CramRecordReader(ValidationStringency.SILENT);
				dataReaderFactory.buildReader(reader, new DefaultBitInputStream(new ByteArrayInputStream(
						slice.coreBlock.getRawContent())), inputMap, c.header, slice.sequenceId);

				for (int i = 0; i < slice.nofRecords; i++) {
					CramCompressionRecord record = new CramCompressionRecord();
					reader.read(record);
					if (record.isSegmentUnmapped() || record.readFeatures == null || record.readFeatures.isEmpty())
						continue;
					for (ReadFeature rf : record.readFeatures) {
						if (rf.getOperator() == ReadBase.operator) {
							System.out.println("Read feature B detected.");
							System.exit(1);
						}
					}
				}
			}
		}
	}
}
