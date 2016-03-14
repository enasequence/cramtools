package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.CramLossyOptions;
import htsjdk.samtools.cram.CramSerilization;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
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

import net.sf.cram.ref.ReferenceSource;

class BamToCram_OBA_Function implements Function<OrderedByteArray, OrderedByteArray> {
	private static Log log = Log.getInstance(BamToCram_OBA_Function.class);
	private CramHeader header;
	private ReferenceSource referenceSource;
	private String captureTags;
	private String ignoreTags;
	private CramLossyOptions lossyOptions;

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

		Container container = null;
		try {
			container = CramSerilization.convert(records, header.getSamFileHeader(), referenceSource, lossyOptions);
		} catch (IllegalArgumentException | IllegalAccessException | IOException e) {
			throw new RuntimeException(e);
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ContainerIO.writeContainer(header.getVersion(), container, baos);
		} catch (IOException e) {
			throw new RuntimeIOException();
		}
		result.bytes = baos.toByteArray();
		result.order = object.order;
		log.debug(String.format("Converted OBA %d, records %d", object.order, records.size()));
		return result;
	}

	public void setCaptureTags(String captureTags) {
		this.captureTags = captureTags;
	}

	public void setIgnoreTags(String ignoreTags) {
		this.ignoreTags = ignoreTags;
	}
}