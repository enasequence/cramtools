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
package net.sf.cram;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.encoding.reader.DataReaderFactory;
import htsjdk.samtools.cram.io.DefaultBitInputStream;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.cram.encoding.reader.AbstractFastqReader;
import htsjdk.samtools.cram.encoding.reader.MultiFastqOutputter;
import htsjdk.samtools.cram.encoding.reader.ReaderToFastq;
import net.sf.cram.ref.ReferenceSource;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

public class Cram2Fastq {
    private static Log log = Log.getInstance(Cram2Fastq.class);
    public static final String COMMAND = "fastq";

    private static void printUsage(JCommander jc) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        jc.usage(sb);

        System.out.println("Version " + Cram2Fastq.class.getPackage().getImplementationVersion());
        System.out.println(sb.toString());
    }

    @SuppressWarnings("restriction")
    public static void main(String[] args) throws Exception {
        Params params = new Params();
        JCommander jc = new JCommander(params);
        try {
            jc.parse(args);
        } catch (Exception e) {
            System.out.println("Failed to parse parameteres, detailed message below: ");
            System.out.println(e.getMessage());
            System.out.println();
            System.out.println("See usage: -h");
            System.exit(1);
        }

        if (args.length == 0 || params.help) {
            printUsage(jc);
            System.exit(1);
        }

        Log.setGlobalLogLevel(params.logLevel);

        SeekableFileStream sfs = new SeekableFileStream(params.cramFile);
        CramHeader cramHeader = CramIO.readCramHeader(sfs);
        ReferenceSource referenceSource = new ReferenceSource(params.reference);
        FixBAMFileHeader fix = new FixBAMFileHeader(referenceSource);
        fix.setConfirmMD5(!params.skipMD5Checks);
        fix.setIgnoreMD5Mismatch(params.ignoreMD5Mismatch);
        fix.fixSequences(cramHeader.getSamFileHeader().getSequenceDictionary().getSequences());
        sfs.seek(0);

        if (params.reference == null)
            log.warn("No reference file specified, remote access over internet may be used to download public sequences. ");

        final AtomicBoolean brokenPipe = new AtomicBoolean(false);
        sun.misc.Signal.handle(new sun.misc.Signal("PIPE"), new sun.misc.SignalHandler() {
            @Override
            public void handle(sun.misc.Signal sig) {
                brokenPipe.set(true);
            }
        });

        CollatingDumper d = new CollatingDumper(sfs, referenceSource, 3, params.fastqBaseName, params.gzip,
                params.maxRecords, params.reverse, params.defaultQS, brokenPipe);
        d.prefix = params.prefix;
        d.run();

        if (d.exception != null)
            throw d.exception;
    }

    private static abstract class Dumper implements Runnable {
        protected InputStream cramIS;
        protected byte[] ref = null;
        protected ReferenceSource referenceSource;
        protected FileOutput[] outputs;
        protected long maxRecords = -1;
        protected CramHeader cramHeader;
        protected Container container;
        protected AbstractFastqReader reader;
        protected Exception exception;
        private boolean reverse = false;
        protected AtomicBoolean brokenPipe;

        public Dumper(InputStream cramIS, ReferenceSource referenceSource, int nofStreams, String fastqBaseName,
                      boolean gzip, long maxRecords, boolean reverse, int defaultQS, AtomicBoolean brokenPipe)
                throws IOException {

            this.cramIS = cramIS;
            this.referenceSource = referenceSource;
            this.maxRecords = maxRecords;
            this.reverse = reverse;
            this.brokenPipe = brokenPipe;
            outputs = new FileOutput[nofStreams];
            for (int index = 0; index < outputs.length; index++)
                outputs[index] = new FileOutput();

            if (fastqBaseName == null) {
                OutputStream joinedOS = System.out;
                if (gzip)
                    joinedOS = (new GZIPOutputStream(joinedOS));
                for (int index = 0; index < outputs.length; index++)
                    outputs[index].outputStream = joinedOS;
            } else {
                String extension = ".fastq" + (gzip ? ".gz" : "");
                String path;
                for (int index = 0; index < outputs.length; index++) {
                    if (index == 0)
                        path = fastqBaseName + extension;
                    else
                        path = fastqBaseName + "_" + index + extension;

                    outputs[index].file = new File(path);
                    OutputStream os = new BufferedOutputStream(new FileOutputStream(outputs[index].file));

                    if (gzip)
                        os = new GZIPOutputStream(os);

                    outputs[index].outputStream = os;
                }
            }
        }

        protected abstract AbstractFastqReader newReader();

        protected abstract void containerHasBeenRead() throws IOException;

        protected void doRun() throws IOException {
            cramHeader = CramIO.readCramHeader(cramIS);
            FixBAMFileHeader fix = new FixBAMFileHeader(referenceSource);

            reader = newReader();
            reader.reverseNegativeReads = reverse;
            MAIN_LOOP:
            while (!brokenPipe.get() && (container = ContainerIO.readContainer(cramHeader.getVersion(),cramIS)) != null) {
                DataReaderFactory f = new DataReaderFactory();

                for (Slice s : container.slices) {
                    if (s.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX && s.sequenceId != -2) {
                        SAMSequenceRecord sequence = cramHeader.getSamFileHeader().getSequence(s.sequenceId);

                        if (sequence == null)
                            throw new RuntimeException("Null sequence for id: " + s.sequenceId);

                        ref = referenceSource.getReferenceBases(sequence, true);

                            if (!s.validateRefMD5(ref)) {
                                log.error(String
                                        .format("Reference sequence MD5 mismatch for slice: seq id %d, start %d, span %d, expected MD5 %s",
                                                s.sequenceId, s.alignmentStart, s.alignmentSpan,
                                                String.format("%032x", new BigInteger(1, s.refMD5))));
                                throw new RuntimeException("Reference checksum mismatch.");
                            }
                    } else
                        ref = new byte[0];

                    Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
                    for (Integer exId : s.external.keySet()) {
                        inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId).getRawContent()));
                    }

                    reader.referenceSequence = ref;
                    reader.prevAlStart = s.alignmentStart;
                    reader.substitutionMatrix = container.header.substitutionMatrix;
                    reader.recordCounter = 0;
                    try {
                        f.buildReader(reader,
                                new DefaultBitInputStream(new ByteArrayInputStream(s.coreBlock.getRawContent())),
                                inputMap, container.header, s.sequenceId);
                    } catch (IllegalArgumentException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }

                    for (int i = 0; i < s.nofRecords; i++) {
                        reader.read();
                        if (maxRecords > -1) {
                            if (maxRecords == 0)
                                break MAIN_LOOP;
                            maxRecords--;
                        }
                    }

                    containerHasBeenRead();
                }
            }
            if (!brokenPipe.get())
                reader.finish();
        }

        @Override
        public void run() {
            try {
                doRun();

                if (outputs != null) {
                    for (FileOutput os : outputs)
                        os.close();
                }
            } catch (Exception e) {
                this.exception = e;
            }
        }
    }

    private static class SimpleDumper extends Dumper {
        public SimpleDumper(InputStream cramIS, ReferenceSource referenceSource, int nofStreams, String fastqBaseName,
                            boolean gzip, int maxRecords, boolean reverse, int defaultQS, AtomicBoolean brokenPipe)
                throws IOException {
            super(cramIS, referenceSource, nofStreams, fastqBaseName, gzip, maxRecords, reverse, defaultQS, brokenPipe);
        }

        @Override
        protected AbstractFastqReader newReader() {
            return new ReaderToFastq();
        }

        @Override
        protected void containerHasBeenRead() throws IOException {
            ReaderToFastq reader = (ReaderToFastq) super.reader;
            for (int i = 0; i < outputs.length; i++) {
                ByteBuffer buf = reader.bufs[i];
                OutputStream os = outputs[i].outputStream;
                buf.flip();
                os.write(buf.array(), 0, buf.limit());
                if (buf.limit() > 0)
                    outputs[i].empty = false;
                buf.clear();
            }
        }
    }

    private static class CollatingDumper extends Dumper {
        private FileOutput fo = new FileOutput();
        private String prefix;
        private long counter = 1;
        private MultiFastqOutputter multiFastqOutputter;
        private int defaultQS;

        public CollatingDumper(InputStream cramIS, ReferenceSource referenceSource, int nofStreams,
                               String fastqBaseName, boolean gzip, long maxRecords, boolean reverse, int defaultQS,
                               AtomicBoolean brokenPipe) throws IOException {
            super(cramIS, referenceSource, nofStreams, fastqBaseName, gzip, maxRecords, reverse, defaultQS, brokenPipe);
            this.defaultQS = defaultQS;
            this.brokenPipe = brokenPipe;
            fo.file = File.createTempFile(fastqBaseName == null ? "overflow.bam" : fastqBaseName + ".overflow.bam",
                    ".tmp");
            fo.file.deleteOnExit();
            fo.outputStream = new BufferedOutputStream(new FileOutputStream(fo.file));
        }

        @Override
        protected AbstractFastqReader newReader() {
            if (multiFastqOutputter != null) {
                counter = multiFastqOutputter.getCounter();
            }
            multiFastqOutputter = new MultiFastqOutputter(outputs, fo, referenceSource, cramHeader.getSamFileHeader(),
                    counter);
            if (prefix != null) {
                multiFastqOutputter.setPrefix(prefix.getBytes());
                // multiFastqOutputter.setCounter(counter);
            }
            multiFastqOutputter.defaultQS = this.defaultQS;
            return multiFastqOutputter;
        }

        @Override
        protected void containerHasBeenRead() throws IOException {
        }

        @Override
        public void doRun() throws IOException {
            super.doRun();

            fo.close();

            if (fo.empty)
                return;

            log.info("Sorting overflow BAM: ", fo.file.length());
            SAMFileReader.setDefaultValidationStringency(ValidationStringency.SILENT);
            SAMFileReader r = new SAMFileReader(fo.file);
            SAMRecordIterator iterator = r.iterator();
            if (!iterator.hasNext()) {
                r.close();
                fo.file.delete();
                return;
            }

            SAMRecord r1 = iterator.next();
            SAMRecord r2 = null;
            counter = multiFastqOutputter.getCounter();
            log.info("Counter=" + counter);
            while (!brokenPipe.get() && iterator.hasNext()) {
                r2 = iterator.next();
                if (r1.getReadName().equals(r2.getReadName())) {
                    print(r1, r2);
                    counter++;
                    r1 = null;
                    if (!iterator.hasNext())
                        break;
                    r1 = iterator.next();
                    r2 = null;
                } else {
                    print(r1, 0);
                    r1 = r2;
                    r2 = null;
                    counter++;
                }
            }
            if (r1 != null)
                print(r1, 0);
            r.close();
            fo.file.delete();
        }

        private void print(SAMRecord r1, SAMRecord r2) throws IOException {
            if (r1.getFirstOfPairFlag()) {
                print(r1, 1);
                print(r2, 2);
            } else {
                print(r1, 2);
                print(r2, 1);
            }
        }

        private void print(SAMRecord r, int index) throws IOException {
            OutputStream os = outputs[index];
            os.write('@');
            if (prefix != null) {
                os.write(prefix.getBytes());
                os.write('.');
                os.write(String.valueOf(counter).getBytes());
                os.write(' ');
            }
            os.write(r.getReadName().getBytes());
            if (index > 0) {
                os.write('/');
                os.write(48 + index);
            }
            os.write('\n');
            os.write(r.getReadBases());
            os.write("\n+\n".getBytes());
            os.write(r.getBaseQualityString().getBytes());
            os.write('\n');
        }
    }

    private static class FileOutput extends OutputStream {
        File file;
        OutputStream outputStream;
        boolean empty = true;

        @Override
        public void write(int b) throws IOException {
            outputStream.write(b);
            empty = false;
        }

        @Override
        public void write(byte[] b) throws IOException {
            outputStream.write(b);
            empty = false;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            empty = false;
        }

        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }

        @Override
        public void close() throws IOException {
            if (outputStream != null && outputStream != System.out && outputStream != System.err) {
                outputStream.close();
                outputStream = null;
            }
            if (empty && file != null && file.exists())
                file.delete();
        }
    }

    @Parameters(commandDescription = "CRAM to FastQ dump conversion. ")
    static class Params {
        @Parameter(names = {"-l", "--log-level"}, description = "Change log level: DEBUG, INFO, WARNING, ERROR.", converter = CramTools.LevelConverter.class)
        Log.LogLevel logLevel = Log.LogLevel.ERROR;

        @Parameter(names = {"-h", "--help"}, description = "Print help and quit")
        boolean help = false;

        @Parameter(names = {"--input-cram-file", "-I"}, converter = FileConverter.class, description = "The path to the CRAM file to uncompress. Omit if standard input (pipe).")
        File cramFile;

        @Parameter(names = {"--reference-fasta-file", "-R"}, converter = FileConverter.class, description = "Path to the reference fasta file, it must be uncompressed and indexed (use 'samtools faidx' for example). ")
        File reference;

        @Parameter(names = {"--fastq-base-name", "-F"}, description = "'_number.fastq[.gz] will be appended to this string to obtain output fastq file name. If this parameter is omitted then all reads are printed with no garanteed order.")
        String fastqBaseName;

        @Parameter(names = {"--gzip", "-z"}, description = "Compress fastq files with gzip.")
        boolean gzip;

        @Parameter(names = {"--reverse"}, description = "Re-reverse reads mapped to negative strand.")
        boolean reverse = false;

        @Parameter(names = {"--enumerate"}, description = "Append read names with read index (/1 for first in pair, /2 for second in pair).")
        boolean appendSegmentIndexToReadNames;

        @Parameter(names = {"--max-records"}, description = "Stop after reading this many records.")
        long maxRecords = -1;

        @Parameter(names = {"--read-name-prefix"}, description = "Replace read names with this prefix and a sequential integer.")
        String prefix = null;

        @Parameter(names = {"--default-quality-score"}, description = "Use this quality score (decimal representation of ASCII symbol) as a default value when the original quality score was lost due to compression. Minimum is 33.")
        int defaultQS = '?';

        @Parameter(names = {"--ignore-md5-mismatch"}, description = "Issue a warning on sequence MD5 mismatch and continue. This does not garantee the data will be read succesfully. ")
        public boolean ignoreMD5Mismatch = false;

        @Parameter(names = {"--skip-md5-check"}, description = "Skip MD5 checks when reading the header.")
        public boolean skipMD5Checks = false;
    }

}
