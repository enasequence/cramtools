package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.build.ContainerFactory;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.RuntimeIOException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import net.sf.cram.Bam2Cram;
import net.sf.cram.ref.ReferenceSource;

class BamToCram_OBA_Function implements Function<OrderedByteArray, OrderedByteArray> {
	private static Log log = Log.getInstance(BamToCram_OBA_Function.class);
	private CramHeader header;
	private ReferenceSource referenceSource;
	private String captureTags;
	private String ignoreTags;

	BamToCram_OBA_Function(CramHeader header, ReferenceSource referenceSource) {
		this.header = header;
		this.referenceSource = referenceSource;
		log.info("converter created");
	}

	@Override
	public OrderedByteArray apply(OrderedByteArray object) {
		if (object == null)
			throw new NullPointerException();

		BAMRecordCodec codec = new BAMRecordCodec(header.getSamFileHeader());
		Set<Integer> refSet = new HashSet<Integer>();

		if (object.bytes == null || object.order < 0) {
			log.error("Empty stripe: " + object);
			throw new IllegalArgumentException();
		}
		codec.setInputStream(new ByteArrayInputStream(object.bytes));
		SAMRecord samRecord = null;
		List<SAMRecord> records = new ArrayList<SAMRecord>();
		while ((samRecord = codec.decode()) != null) {
			records.add(samRecord);
			refSet.add(samRecord.getReferenceIndex());
		}

		OrderedByteArray result = new OrderedByteArray();
		if (records.isEmpty()) {
			log.error("No records in stripe: " + object);
			result.bytes = new byte[0];
			return result;
		}

		int refId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
		if (refSet.size() == 1) {
			refId = refSet.iterator().next();
		} else {
			if (refSet.remove(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX))
				refId = refSet.iterator().next();
			else {
				// multiref detected
				throw new RuntimeException("Multiref not supported.");
			}
		}
		byte[] ref = null;
		if (refId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
			ref = new byte[] {};
		else
			ref = referenceSource.getReferenceBases(header.getSamFileHeader().getSequence(refId), true);

		List<CramCompressionRecord> cramRecords = Bam2Cram.convert(records, header, ref, null, null, true, captureTags,
				ignoreTags);
		int detached = 0;
		for (CramCompressionRecord r : cramRecords) {
			if (r.isDetached())
				detached++;
		}
		ContainerFactory f = new ContainerFactory(header.getSamFileHeader(), cramRecords.size());
		Container container;
		try {
			container = f.buildContainer(cramRecords);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		for (Slice s : container.slices) {
			s.setRefMD5(ref);
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ContainerIO.writeContainer(header.getVersion(), container, baos);
		} catch (IOException e) {
			throw new RuntimeIOException();
		}
		result.bytes = baos.toByteArray();
		result.order = object.order;
		log.debug(String.format("Converted OBA %d, records %d, detached %d", object.order, cramRecords.size(), detached));
		return result;
	}

	public void setCaptureTags(String captureTags) {
		this.captureTags = captureTags;
	}

	public void setIgnoreTags(String ignoreTags) {
		this.ignoreTags = ignoreTags;
	}
}