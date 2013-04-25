package net.sf.cram.build;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.cram.encoding.DataReaderFactory;
import net.sf.cram.encoding.DataReaderFactory.DataReaderWithStats;
import net.sf.cram.encoding.Reader;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.cram.structure.CompressionHeader;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramRecord;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.picard.util.Log.LogLevel;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;

public class ContainerParser {
	private static Log log = Log.getInstance(ContainerParser.class);

	private SAMFileHeader samFileHeader;
	private Map<String, Long> nanoMap = new TreeMap<String, Long>();

	public ContainerParser(SAMFileHeader samFileHeader) {
		this.samFileHeader = samFileHeader;
	}

	public List<CramRecord> getRecords(Container container,
			ArrayList<CramRecord> records) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		long time1 = System.nanoTime();
		if (records == null)
			records = new ArrayList<CramRecord>(container.nofRecords);

		for (Slice s : container.slices)
			records.addAll(getRecords(s, container.h));

		long time2 = System.nanoTime();

		container.parseTime = time2 - time1;

		if (log.isEnabled(LogLevel.DEBUG)) {
			for (String key : nanoMap.keySet()) {
				log.debug(String.format("%s: %dms.", key, nanoMap.get(key)
						.longValue() / 1000000));
			}
		}

		return records;
	}

	public List<CramRecord> getRecords(Slice s, CompressionHeader h) throws IllegalArgumentException,
			IllegalAccessException, IOException {
		String seqName = SAMRecord.NO_ALIGNMENT_REFERENCE_NAME;
		if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			SAMSequenceRecord sequence = samFileHeader.getSequence(s.sequenceId);
			seqName = sequence.getSequenceName();
		}
		DataReaderFactory f = new DataReaderFactory();
		Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
		for (Integer exId : s.external.keySet()) {
			inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId)
					.getRawContent()));
		}

		long time = 0;
		Reader reader = new Reader() ;
		f.buildReader(reader, new DefaultBitInputStream(
				new ByteArrayInputStream(s.coreBlock.getRawContent())),
				inputMap, h, s.sequenceId);

		List<CramRecord> records = new ArrayList<CramRecord>();

		long readNanos = 0;
		int prevStart = s.alignmentStart;
		for (int i = 0; i < s.nofRecords; i++) {
			CramRecord r = new CramRecord();
			r.index = i;

			try {
				time = System.nanoTime();
				reader.read(r);
				readNanos += System.nanoTime() - time;
			} catch (EOFException e) {
				e.printStackTrace();
				throw e;
			}

			if (r.sequenceId == s.sequenceId) {
				r.sequenceName = seqName;
				r.sequenceId = s.sequenceId;
			} else {
				if (r.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
					r.sequenceName = SAMRecord.NO_ALIGNMENT_REFERENCE_NAME;
				else {
					String name = samFileHeader.getSequence(r.sequenceId)
							.getSequenceName();
					r.sequenceName = name;
				}
			}

			records.add(r);

			if (h.AP_seriesDelta) {
				prevStart += r.alignmentDelta;
				r.alignmentStart = prevStart;
			}
		}
		log.debug("Slice records read time: " + readNanos / 1000000);

		Map<String, DataReaderWithStats> statMap = f.getStats(reader);
		for (String key : statMap.keySet()) {
			long value = 0;
			if (!nanoMap.containsKey(key)) {
				nanoMap.put(key, 0L);
				value = 0;
			} else
				value = nanoMap.get(key);
			nanoMap.put(key, value + statMap.get(key).nanos);
		}

		return records;
	}
}
