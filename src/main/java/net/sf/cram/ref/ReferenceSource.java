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
package net.sf.cram.ref;

import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.Log;
import net.sf.cram.common.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * A central class for automated discovery of reference sequences. The algorithm
 * is expected similar to that of samtools:
 * <ul>
 * <li>
 * Search in memory cache by sequence name.</li>
 * <li>
 * Use local fasta file is supplied as a reference file and cache the found
 * sequence in memory.</li>
 * <li>
 * Try REF_CACHE env variable.</li>
 * <li>
 * Try all entries in REF_PATH. The default value is the EBI reference service.</li>
 * <li>
 * Try @SQ:UR as a URL for a fasta file with the fasta index next to it.</li>
 * </ul>
 *
 * @author vadim
 */
public class ReferenceSource extends htsjdk.samtools.cram.ref.ReferenceSource {
    private static final int REF_BASES_TO_CHECK_FOR_SANITY = 1000;
    private static final Pattern chrPattern = Pattern.compile("chr.*", Pattern.CASE_INSENSITIVE);
    private static String REF_CACHE = System.getenv("REF_CACHE");
    private static String REF_PATH = System.getenv("REF_PATH");
    private static List<PathPattern> refPatterns = new ArrayList<PathPattern>();

    static {
        if (REF_PATH == null)
            REF_PATH = "http://www.ebi.ac.uk/ena/cram/md5/%s";

        if (REF_CACHE != null)
            refPatterns.add(new PathPattern(REF_CACHE));
        for (String s : REF_PATH.split("(?i)(?<!(http|ftp)):"))
            refPatterns.add(new PathPattern(s));
    }

    private static Log log = Log.getInstance(ReferenceSource.class);
    private ReferenceSequenceFile rsFile;
    private FastaSequenceIndex fastaSequenceIndex;
    private int downloadTriesBeforeFailing = 2;

    /*
     * In-memory cache of ref bases by sequence name. Garbage collector will
     * automatically clean it if memory is low.
     */
    private Map<String, WeakReference<byte[]>> cacheW = new HashMap<String, WeakReference<byte[]>>();

    public ReferenceSource() {
    }

    public ReferenceSource(File file) {
        if (file != null) {
            rsFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(file);

            File indexFile = new File(file.getAbsoluteFile() + ".fai");
            if (indexFile.exists())
                fastaSequenceIndex = new FastaSequenceIndex(indexFile);
        }
    }

    public ReferenceSource(ReferenceSequenceFile rsFile) {
        this.rsFile = rsFile;
    }

    public void clearCache() {
        cacheW.clear();
    }

    private byte[] findInCache(String name) {
        WeakReference<byte[]> r = cacheW.get(name);
        if (r != null) {
            byte[] bytes = r.get();
            if (bytes != null)
                return bytes;
        }
        return null;
    }

    public synchronized byte[] getReferenceBases(SAMSequenceRecord record, boolean tryNameVariants) {
        byte[] bases = findBases(record, tryNameVariants);
        if (bases == null)
            return null;

        cacheW.put(record.getSequenceName(), new WeakReference<byte[]>(bases));

        String md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG);
        if (md5 == null) {
            md5 = Utils.calculateMD5String(bases);
            record.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
        }

        if (REF_CACHE != null)
            addToRefCache(md5, bases);

        return bases;
    }

    protected byte[] findBases(SAMSequenceRecord record, boolean tryNameVariants) {
        { // check cache by sequence name:
            String name = record.getSequenceName();
            byte[] bases = findInCache(name);
            if (bases != null) {
                log.debug("Reference found in memory cache by name: " + name);
                return bases;
            }
        }

        String md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG);
        { // check cache by md5:
            if (md5 != null) {
                byte[] bases = findInCache(md5);
                if (bases != null) {
                    log.debug("Reference found in memory cache by md5: " + md5);
                    return bases;
                }
            }
        }

        byte[] bases;

        { // try to fetch sequence by name:
            bases = findBasesByName(record.getSequenceName(), tryNameVariants);
            if (bases != null) {
                Utils.upperCase(bases);
                return bases;
            }
        }

        { // try to fetch sequence by md5:
            if (md5 != null)
                try {
                    bases = findBasesByMD5(md5);
                } catch (Exception e) {
                    if (e instanceof RuntimeException)
                        throw (RuntimeException) e;
                    throw new RuntimeException(e);
                }
            if (bases != null) {
                return bases;
            }
        }

        { // try @SQ:UR file location
            if (record.getAttribute(SAMSequenceRecord.URI_TAG) != null) {
                ReferenceSequenceFromSeekable s = ReferenceSequenceFromSeekable.fromString(record
                        .getAttribute(SAMSequenceRecord.URI_TAG));
                bases = s.getSubsequenceAt(record.getSequenceName(), 1, record.getSequenceLength());
                Utils.upperCase(bases);
                return bases;
            }
        }
        return null;
    }

    protected byte[] findBasesByName(String name, boolean tryVariants) {
        if (rsFile == null || !rsFile.isIndexed())
            return null;

        ReferenceSequence sequence = null;
        if (fastaSequenceIndex != null)
            if (fastaSequenceIndex.hasIndexEntry(name))
                sequence = rsFile.getSequence(name);
            else
                sequence = null;

        if (sequence != null)
            return sequence.getBases();

        if (tryVariants) {
            for (String variant : getVariants(name)) {
                try {
                    sequence = rsFile.getSequence(variant);
                } catch (Exception e) {
                    log.info("Sequence not found: " + variant);
                }
                if (sequence != null) {
                    log.debug("Reference found in memory cache for name %s by variant %s", name, variant);
                    return sequence.getBases();
                }
            }
        }
        return null;
    }

    /**
     * @param path
     * @return true if the path is a valid URL, false otherwise.
     */
    private static boolean isURL(String path) {
        try {
            URL url = new URL(path);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    private byte[] loadFromPath(String path, String md5) throws IOException {
        if (isURL(path)) {
            URL url = new URL(path);
            for (int i = 0; i < downloadTriesBeforeFailing; i++) {
                InputStream is = url.openStream();
                if (is == null)
                    return null;

                byte[] data = InputStreamUtils.readFully(is);

                if (confirmMD5(md5, data)) {
                    // sanitize, Internet is a wild place:
                    if (Utils.isValidSequence(data, REF_BASES_TO_CHECK_FOR_SANITY))
                        return data;
                    else {
                        // reject, it looks like garbage
                        log.error("Downloaded sequence looks suspicous, rejected: " + url.toExternalForm());
                        break;
                    }
                }
            }
        } else {
            File file = new File(path);
            if (file.exists()) {
                byte[] data = InputStreamUtils.readFully(new FileInputStream(file));

                if (confirmMD5(md5, data))
                    return data;
                else
                    throw new RuntimeException("MD5 mismatch for cached file: " + file.getAbsolutePath());
            }
        }
        return null;
    }

    protected byte[] findBasesByMD5(String md5) throws MalformedURLException, IOException {
        for (PathPattern p : refPatterns) {
            String path = p.format(md5);
            byte[] data = loadFromPath(path, md5);
            if (data == null)
                continue;
            log.debug("Reference found at the location ", path);
            return data;
        }

        return null;
    }

    private static void addToRefCache(String md5, byte[] data) {
        File cachedFile = new File(new PathPattern(REF_CACHE).format(md5));
        if (!cachedFile.exists()) {
            log.debug(String.format("Adding to REF_CACHE: md5=%s, length=%d", md5, data.length));
            cachedFile.getParentFile().mkdirs();
            File tmpFile;
            try {
                tmpFile = File.createTempFile(md5, ".tmp", cachedFile.getParentFile());
                FileOutputStream fos = new FileOutputStream(tmpFile);
                fos.write(data);
                fos.close();
                tmpFile.renameTo(cachedFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ReferenceSource s = new ReferenceSource();
        SAMSequenceRecord record = new SAMSequenceRecord("20", 1518);
        byte[] bases = s.getReferenceBases(record, false);
        assertThat(bases, is(nullValue()));

        String md5 = "0000259a289a94ef5f10220539b79e7e";
        record.setAttribute(SAMSequenceRecord.MD5_TAG, md5);
        bases = s.getReferenceBases(record, false);
        assertThat(bases, is(notNullValue()));
        assertThat(bases.length, is(record.getSequenceLength()));
        assertThat(Utils.calculateMD5String(bases), is(record.getAttribute(SAMSequenceRecord.MD5_TAG)));
        assertThat(s.cacheW.containsKey(record.getSequenceName()), is(true));
        assertThat(s.cacheW.containsKey(md5), is(false));
    }

    private boolean confirmMD5(String md5, byte[] data) {
        String downloadedMD5 = null;
        downloadedMD5 = Utils.calculateMD5String(data);
        if (md5.equals(downloadedMD5)) {
            return true;
        } else {
            String message = String.format("Downloaded sequence is corrupt: requested md5=%s, received md5=%s", md5,
                    downloadedMD5);
            log.error(message);
            return false;
        }
    }

    protected List<String> getVariants(String name) {
        List<String> variants = new ArrayList<String>();

        if (name.equals("M"))
            variants.add("MT");

        if (name.equals("MT"))
            variants.add("M");

        boolean chrPatternMatch = chrPattern.matcher(name).matches();
        if (chrPatternMatch)
            variants.add(name.substring(3));
        else
            variants.add("chr" + name);

        if ("chrM".equals(name)) {
            // chrM case:
            variants.add("MT");
        }
        return variants;
    }

    public int getDownloadTriesBeforeFailing() {
        return downloadTriesBeforeFailing;
    }

    public void setDownloadTriesBeforeFailing(int downloadTriesBeforeFailing) {
        this.downloadTriesBeforeFailing = downloadTriesBeforeFailing;
    }
}
