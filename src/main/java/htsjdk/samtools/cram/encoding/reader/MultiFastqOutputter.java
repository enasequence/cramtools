package htsjdk.samtools.cram.encoding.reader;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.util.Log;
import net.sf.cram.ref.ReferenceSource;

public class MultiFastqOutputter extends AbstractFastqReader {
	private static final Log log = Log.getInstance(MultiFastqOutputter.class);
	private Map<FastqRead, FastqRead> readSet = new TreeMap<FastqRead, FastqRead>();
	private int maxCacheSize = Integer
			.parseInt(System.getProperty("fastq-dumper.cache-size", Integer.toString(100000)));

	private long generation = 0;
	private OutputStream[] streams;

	private SAMFileHeader headerForOverflowWriter;
	private SAMFileWriter writer;
	private OutputStream cacheOverFlowStream;
	private byte[] prefix;
	private long counter = 1;
	private ReferenceSource referenceSource;
	private SAMFileHeader header;

	public MultiFastqOutputter(OutputStream[] streams, OutputStream cacheOverFlowStream,
			ReferenceSource referenceSource, SAMFileHeader header, long counter) {
		this.streams = streams;
		this.cacheOverFlowStream = cacheOverFlowStream;
		this.referenceSource = referenceSource;
		this.header = header;
		super.counterOffset = counter;
	}

	public byte[] getPrefix() {
		return prefix;
	}

	public void setPrefix(byte[] prefix) {
		this.prefix = prefix;
	}

	public long getCounter() {
		return counter;
	}

	protected void write(FastqRead read, OutputStream stream) throws IOException {
		if (prefix == null) {
			stream.write(read.data);
		} else {
			streams[read.templateIndex].write('@');
			streams[read.templateIndex].write(prefix);
			streams[read.templateIndex].write('.');
			streams[read.templateIndex].write(String.valueOf(counter).getBytes());
			streams[read.templateIndex].write(' ');
			streams[read.templateIndex].write(read.data, 1, read.data.length - 1);
		}
	}

	protected void foundCollision(FastqRead read) {
		FastqRead anchor = readSet.remove(read);
		try {
			write(anchor, streams[anchor.templateIndex]);
			write(read, streams[read.templateIndex]);
			counter++;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Comparator<FastqRead> byGenerationComparator = new Comparator<FastqRead>() {

		@Override
		public int compare(FastqRead o1, FastqRead o2) {
			return (int) (o1.generation - o2.generation);
		}
	};

	protected void kickedFromCache(FastqRead read) {
		if (writer == null) {
			log.info("Creating overflow BAM file.");
			headerForOverflowWriter = header.clone();
			headerForOverflowWriter.setSortOrder(SAMFileHeader.SortOrder.queryname);

			writer = new SAMFileWriterFactory().makeBAMWriter(headerForOverflowWriter, false, cacheOverFlowStream);
		}
		SAMRecord r = read.toSAMRecord(writer.getFileHeader());
		writer.addAlignment(r);
	}

	List<FastqRead> list = new ArrayList<FastqRead>();

	protected void purgeCache() {
		long time1 = System.nanoTime();
		list.clear();
		for (FastqRead read : readSet.keySet())
			list.add(read);

		Collections.sort(list, byGenerationComparator);
		for (int i = 0; i < list.size() / 2; i++) {
			readSet.remove(list.get(i));
			kickedFromCache(list.get(i));
		}

		list.clear();
		long time2 = System.nanoTime();
		log.debug(String.format("Cache purged in %.2fms.\n", (time2 - time1) / 1000000f));
	}

	@Override
	protected void writeRead(byte[] name, int flags, byte[] bases, byte[] scores) {
		FastqRead read = new FastqRead(readLength, name, appendSegmentIndexToReadNames,
				getSegmentIndexInTemplate(flags), bases, scores);
		read.generation = generation++;
		if (read.templateIndex == 0) {
			try {
				write(read, streams[0]);
				counter++;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return;
		}

		if (readSet.containsKey(read)) {
			foundCollision(read);
		} else {
			readSet.put(read, read);

			if (readSet.size() > maxCacheSize)
				purgeCache();
		}
	}

	@Override
	public void finish() {
		for (FastqRead read : readSet.keySet())
			kickedFromCache(read);

		readSet.clear();
		if (writer != null)
			writer.close();
		writer = null;
	}

	@Override
	protected byte[] refSeqChanged(int seqID) {
		SAMSequenceRecord sequence = header.getSequence(seqID);
		return referenceSource.getReferenceBases(sequence, true);
	}
}
