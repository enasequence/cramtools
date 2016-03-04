package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.ContainerParser;
import htsjdk.samtools.cram.build.Cram2SamRecordFactory;
import htsjdk.samtools.cram.build.CramNormalizer;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.Log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

import net.sf.cram.ref.ReferenceSource;

class CramToBam_OBA_Function implements Function<OrderedByteArray, OrderedByteArray> {
	private static Log log = Log.getInstance(CramToBam_OBA_Function.class);
	private CramHeader header;
	private ContainerParser parser;
	private Cram2SamRecordFactory f;
	private BAMRecordCodec codec;
	private CramNormalizer n;

	CramToBam_OBA_Function(CramHeader header, ReferenceSource referenceSource) {
		this.header = header;
		parser = new ContainerParser(header.getSamFileHeader());
		f = new Cram2SamRecordFactory(header.getSamFileHeader());
		codec = new BAMRecordCodec(header.getSamFileHeader());
		n = new CramNormalizer(header.getSamFileHeader(), referenceSource);
		log.info("converter created");
	}

	@Override
	public OrderedByteArray apply(OrderedByteArray object) {
		if (object == null)
			throw new NullPointerException();

		log.debug("processing container " + object.order);
		Container container;
		try {
			container = ContainerIO.readContainer(header.getVersion(), new ByteArrayInputStream(object.bytes));
			if (container.isEOF())
				return null;

			ArrayList<CramCompressionRecord> records = new ArrayList<CramCompressionRecord>(container.nofRecords);
			parser.getRecords(container, records, ValidationStringency.SILENT);
			n.normalize(records, null, 0, container.header.substitutionMatrix);

			ByteArrayOutputStream bamBAOS = new ByteArrayOutputStream();
			BlockCompressedOutputStream os = new BlockCompressedOutputStream(bamBAOS, null);
			codec.setOutputStream(os);
			for (CramCompressionRecord record : records) {
				SAMRecord samRecord = f.create(record);
				codec.encode(samRecord);
			}
			os.flush();
			OrderedByteArray bb = new OrderedByteArray();
			bb.bytes = bamBAOS.toByteArray();
			bb.order = object.order;
			log.debug(String.format("Converted OBA %d, records %d", object.order, records.size()));
			return bb;
		} catch (IOException | IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}