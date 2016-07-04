package htsjdk.samtools;

import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.cram.CramContext;
import htsjdk.samtools.cram.CramLossyOptions;
import htsjdk.samtools.cram.CramSerilization;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.common.Version;
import htsjdk.samtools.cram.ref.ReferenceTracks;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.RuntimeIOException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.cram.ref.ReferenceSource;

/**
 * Class for writing SAMRecords into a series of CRAM containers on an output
 * stream.
 */
public class CRAMContainerStreamWriter {
	private static final Log log = Log.getInstance(CRAMContainerStreamWriter.class);
	protected static final Version cramVersion = CramVersions.CRAM_v3;

	private int minSingeRefRecords = 1000;
	protected int containerSize = 10000;
	protected static final int REF_SEQ_INDEX_NOT_INITIALIZED = -3;

	protected final SAMFileHeader samFileHeader;
	private final String cramID;
	protected final OutputStream outputStream;
	protected ReferenceSource source;

	protected final List<SAMRecord> samRecords = new ArrayList<SAMRecord>();

	protected CRAMBAIIndexer indexer;
	protected long offset;
	protected CramLossyOptions lossyOptions = CramLossyOptions.lossless();
	private CramContext context;
	private Set<Integer> refIdSet = new HashSet<Integer>();

	public CRAMContainerStreamWriter(final OutputStream outputStream, final OutputStream indexStream,
			final ReferenceSource source, final SAMFileHeader samFileHeader, final String cramId,
			CramLossyOptions lossyOptions) {
		this.outputStream = outputStream;
		this.samFileHeader = samFileHeader;
		this.cramID = cramId;
		this.source = source;
		if (indexStream != null) {
			indexer = new CRAMBAIIndexer(indexStream, samFileHeader);
		}
		context = new CramContext(samFileHeader, source, lossyOptions);
	}

	/**
	 * Create a CRAMContainerStreamWriter for writing SAM records into a series
	 * of CRAM containers on output stream, with an optional index.
	 *
	 * @param outputStream
	 *            where to write the CRAM stream.
	 * @param indexStream
	 *            where to write the output index. Can be null if no index is
	 *            required.
	 * @param source
	 *            reference source
	 * @param samFileHeader
	 *            {@link SAMFileHeader} to be used. Sort order is determined by
	 *            the sortOrder property of this arg.
	 * @param cramId
	 *            used for display in error message display
	 */
	public CRAMContainerStreamWriter(final OutputStream outputStream, final OutputStream indexStream,
			final ReferenceSource source, final SAMFileHeader samFileHeader, final String cramId) {
		this.outputStream = outputStream;
		this.samFileHeader = samFileHeader;
		this.cramID = cramId;
		this.source = source;
		if (indexStream != null) {
			indexer = new CRAMBAIIndexer(indexStream, samFileHeader);
		}
	}

	/**
	 * Write an alignment record.
	 * 
	 * @param alignment
	 *            must not be null
	 */
	public void writeAlignment(final SAMRecord alignment) {
		if (shouldFlushContainer(alignment)) {
			try {
				flushContainer();
			} catch (IOException e) {
				throw new RuntimeIOException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		samRecords.add(alignment);
		refIdSet.add(alignment.getReferenceIndex());
	}

	/**
	 * Write a CRAM file header and SAM header to the stream.
	 * 
	 * @param header
	 *            SAMFileHeader to write
	 */
	public void writeHeader(final SAMFileHeader header) {
		// TODO: header must be written exactly once per writer life cycle.
		offset = CramIO.writeHeader(cramVersion, outputStream, header, cramID);
	}

	/**
	 * Finish writing to the stream. Flushes the record cache and optionally
	 * emits an EOF container.
	 * 
	 * @param writeEOFContainer
	 *            true if an EOF container should be written. Only use false if
	 *            writing a CRAM file fragment which will later be aggregated
	 *            into a complete CRAM file.
	 */
	public void finish(final boolean writeEOFContainer) {
		try {
			if (!samRecords.isEmpty()) {
				flushContainer();
			}
			if (writeEOFContainer) {
				CramIO.issueEOF(cramVersion, outputStream);
			}
			outputStream.flush();
			if (indexer != null) {
				indexer.finish();
			}
			outputStream.close();
		} catch (final IOException e) {
			throw new RuntimeIOException(e);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public CramLossyOptions getLossyOptions() {
		return lossyOptions;
	}

	/**
	 * Decide if the current container should be completed and flushed. The
	 * decision is based on a) number of records and b) if the reference
	 * sequence id has changed.
	 *
	 * @param nextRecord
	 *            the record to be added into the current or next container
	 * @return true if the current container should be flushed and the following
	 *         records should go into a new container; false otherwise.
	 */
	protected boolean shouldFlushContainer(final SAMRecord nextRecord) {
		if (refIdSet.isEmpty()) {
			return false;
		}

		if (samFileHeader.getSortOrder() != SAMFileHeader.SortOrder.coordinate) {
			return samRecords.size() >= containerSize;
		}

		boolean newRef = !refIdSet.contains(nextRecord.getReferenceIndex());
		int seenRefs = refIdSet.size();

		if (newRef && nextRecord.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			// separate unsorted reads
			return true;
		}

		if (newRef && seenRefs == 1) {
			return samRecords.size() >= minSingeRefRecords;
		}

		return samRecords.size() >= containerSize;
	}

	/**
	 * Complete the current container and flush it to the output stream.
	 *
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	protected void flushContainer() throws IllegalArgumentException, IllegalAccessException, IOException {
		if (context.lossyOptions.areReferenceTracksRequired()) {
			for (int refId : refIdSet) {
				if (refId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX)
					continue;
				ReferenceTracks tracks = context.tracks.get(refId);
				if (tracks == null) {
					SAMSequenceRecord sequence = samFileHeader.getSequence(refId);
					if (sequence == null)
						throw new RuntimeException("Sequence not found for id " + refId);

					byte[] bases = context.referenceSource.getReferenceBases(sequence, true);
					if (bases == null)
						throw new RuntimeException("Bases not found for id " + refId);
					tracks = new ReferenceTracks(refId, sequence.getSequenceName(),
							context.referenceSource.getReferenceBases(sequence, true));
					context.tracks.put(refId, tracks);
				}
			}
		}
		Container container = CramSerilization.convert(samRecords, context);
		container.offset = offset;
		offset += ContainerIO.writeContainer(cramVersion, container, outputStream);
		if (indexer != null) {
			/**
			 * Using silent validation here because the reads have been through
			 * validation already or they have been generated somehow through
			 * the htsjdk.
			 */
			indexer.processContainer(container, ValidationStringency.SILENT);
		}
		samRecords.clear();

		if (context.lossyOptions.areReferenceTracksRequired() && samFileHeader.getSortOrder() == SortOrder.coordinate) {
			// remove obsolete tracks: those that will not be needed anymore
			// based on alignment order

			if (refIdSet.isEmpty()) {
				System.out.println("Unmapped reads, clearing all tracks: " + context.tracks.size());
				context.tracks.clear();
			} else {
				if (context.tracks.size() > 1) {
					int maxRefId = -1;
					for (int refId : context.tracks.keySet()) {
						maxRefId = Math.max(maxRefId, refId);
					}
					ReferenceTracks lastTrack = context.tracks.get(maxRefId);
					log.info("Clearing tracks, size=" + context.tracks.size() + "; keeping seq id " + maxRefId);
					context.tracks.clear();
					context.tracks.put(maxRefId, lastTrack);
				}
			}

		}
		refIdSet.clear();

	}

	public void setMinSingeRefRecords(int minSingeRefRecords) {
		this.minSingeRefRecords = minSingeRefRecords;
	}
}
