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
package htsjdk.samtools;

import htsjdk.samtools.cram.lossy.PreservationPolicy;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;
import htsjdk.samtools.util.StringLineReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import net.sf.cram.ref.ReferenceSource;

@SuppressWarnings("UnusedDeclaration")
public class CRAMFileWriter extends SAMFileWriterImpl {
	private CRAMContainerStreamWriter cramContainerStream;
	private final SAMFileHeader samFileHeader;
	private final String fileName;

	private static final Log log = Log.getInstance(CRAMFileWriter.class);

	/**
	 * Create a CRAMFileWriter on an output stream. Requires input records to be
	 * presorted to match the sort order defined by the input
	 * {@code samFileHeader}.
	 *
	 * @param outputStream
	 *            where to write the output. Can not be null.
	 * @param referenceSource
	 *            reference source. Can not be null.
	 * @param samFileHeader
	 *            {@link SAMFileHeader} to be used. Can not be null. Sort order
	 *            is determined by the sortOrder property of this arg.
	 * @param fileName
	 *            used for display in error messages
	 *
	 * @throws IllegalArgumentException
	 *             if the {@code outputStream}, {@code referenceSource} or
	 *             {@code samFileHeader} are null
	 */
	public CRAMFileWriter(final OutputStream outputStream, final ReferenceSource referenceSource,
			final SAMFileHeader samFileHeader, final String fileName, final int maxThreads) {
		this(outputStream, null, referenceSource, samFileHeader, fileName, maxThreads); // defaults
		// to
		// presorted
		// ==
		// true
	}

	/**
	 * Create a CRAMFileWriter and optional index on output streams. Requires
	 * input records to be presorted to match the sort order defined by the
	 * input {@code samFileHeader}.
	 *
	 * @param outputStream
	 *            where to write the output. Can not be null.
	 * @param indexOS
	 *            where to write the output index. Can be null if no index is
	 *            required.
	 * @param referenceSource
	 *            reference source
	 * @param samFileHeader
	 *            {@link SAMFileHeader} to be used. Can not be null. Sort order
	 *            is determined by the sortOrder property of this arg.
	 * @param fileName
	 *            used for display in error messages
	 *
	 * @throws IllegalArgumentException
	 *             if the {@code outputStream}, {@code referenceSource} or
	 *             {@code samFileHeader} are null
	 */
	public CRAMFileWriter(final OutputStream outputStream, final OutputStream indexOS,
			final ReferenceSource referenceSource, final SAMFileHeader samFileHeader, final String fileName,
			int maxThreads) {
		this(outputStream, indexOS, true, referenceSource, samFileHeader, fileName, maxThreads); // defaults
		// to
		// presorted==true
	}

	/**
	 * Create a CRAMFileWriter and optional index on output streams.
	 *
	 * @param outputStream
	 *            where to write the output. Can not be null.
	 * @param indexOS
	 *            where to write the output index. Can be null if no index is
	 *            required.
	 * @param presorted
	 *            if true records written to this writer must already be sorted
	 *            in the order specified by the header
	 * @param referenceSource
	 *            reference source
	 * @param samFileHeader
	 *            {@link SAMFileHeader} to be used. Can not be null. Sort order
	 *            is determined by the sortOrder property of this arg.
	 * @param fileName
	 *            used for display in error message display
	 *
	 * @throws IllegalArgumentException
	 *             if the {@code outputStream}, {@code referenceSource} or
	 *             {@code samFileHeader} are null
	 */
	public CRAMFileWriter(final OutputStream outputStream, final OutputStream indexOS, final boolean presorted,
			final ReferenceSource referenceSource, final SAMFileHeader samFileHeader, final String fileName,
			int maxThreads) {
		if (outputStream == null) {
			throw new IllegalArgumentException("CRAMWriter output stream can not be null.");
		}
		if (referenceSource == null) {
			throw new IllegalArgumentException("A reference is required for CRAM writers");
		}
		if (samFileHeader == null) {
			throw new IllegalArgumentException("A valid SAMFileHeader is required for CRAM writers");
		}
		this.samFileHeader = samFileHeader;
		this.fileName = fileName;
		setSortOrder(samFileHeader.getSortOrder(), presorted);

		cramContainerStream = new CRAMContainerAsynchWriter(outputStream, indexOS, referenceSource, samFileHeader,
				fileName, maxThreads);
		setHeader(samFileHeader);
	}

	public static void main(String[] args) throws IOException {

		Log.setGlobalLogLevel(LogLevel.INFO);
		File bamFile = new File(args[0]);
		File outCramFile = new File(args[1]);
		ReferenceSource source = new ReferenceSource(new File(args[2]));
		int maxThreads = Integer.valueOf(args[3]);

		BAMFileReader reader = new BAMFileReader(bamFile, null, false, ValidationStringency.SILENT,
				new DefaultSAMRecordFactory());
		OutputStream os = new FileOutputStream(outCramFile);
		CRAMFileWriter writer = new CRAMFileWriter(os, source, reader.getFileHeader(), outCramFile.getName(),
				maxThreads);

		CloseableIterator<SAMRecord> iterator = reader.getIterator();

		while (iterator.hasNext()) {
			SAMRecord record = iterator.next();
			writer.addAlignment(record);
		}
		writer.close();
		reader.close();
	}

	/**
	 * Write an alignment record.
	 * 
	 * @param alignment
	 *            must not be null and must have a valid SAMFileHeader.
	 */
	@Override
	protected void writeAlignment(final SAMRecord alignment) {
		cramContainerStream.writeAlignment(alignment);
	}

	@Override
	protected void writeHeader(final String textHeader) {
		cramContainerStream.writeHeader(new SAMTextHeaderCodec().decode(new StringLineReader(textHeader),
				fileName != null ? fileName : null));
	}

	@Override
	protected void finish() {
		cramContainerStream.finish(true); // flush the last container and issue
											// EOF
	}

	@Override
	protected String getFilename() {
		return fileName;
	}

	public boolean isPreserveReadNames() {
		return cramContainerStream.lossyOptions.isPreserveReadNames();
	}

	public void setPreserveReadNames(final boolean preserveReadNames) {
		cramContainerStream.lossyOptions.setPreserveReadNames(preserveReadNames);
	}

	public List<PreservationPolicy> getPreservationPolicies() {
		return cramContainerStream.lossyOptions.getPreservation().getPreservationPolicies();
	}

	public boolean isCaptureAllTags() {
		return cramContainerStream.lossyOptions.isCaptureAllTags();
	}

	public void setCaptureAllTags(final boolean captureAllTags) {
		cramContainerStream.lossyOptions.setCaptureAllTags(captureAllTags);
	}

	public Set<String> getCaptureTags() {
		return cramContainerStream.lossyOptions.getCaptureTags();
	}

	public void setCaptureTags(final Set<String> captureTags) {
		cramContainerStream.lossyOptions.setCaptureTags(captureTags);
	}

	public Set<String> getIgnoreTags() {
		return cramContainerStream.lossyOptions.getIgnoreTags();
	}

	public void setIgnoreTags(final Set<String> ignoreTags) {
		cramContainerStream.lossyOptions.setIgnoreTags(ignoreTags);
	}
}
