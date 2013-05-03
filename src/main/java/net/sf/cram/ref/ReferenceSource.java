package net.sf.cram.ref;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.cram.io.ByteBufferUtils;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMSequenceRecord;

public class ReferenceSource {
	private static Log log = Log.getInstance(ReferenceSource.class);
	private ReferenceSequenceFile rsFile;
	
	private Map<String, byte[]> cache = new HashMap<String, byte[]>(); 

	public ReferenceSource() {
	}

	public ReferenceSource(File file) {
		if (file != null)
			rsFile = ReferenceSequenceFileFactory
					.getReferenceSequenceFile(file);
	}

	public ReferenceSource(ReferenceSequenceFile rsFile) {
		this.rsFile = rsFile;
	}
	
	public void clearCache () {
		cache.clear() ;
	}

	public byte[] getReferenceBases(SAMSequenceRecord record,
			boolean tryNameVariants) {
		if (cache.containsKey(record.getSequenceName())) return cache.get(record.getSequenceName()) ;
		
		String md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG);
		if (md5 != null && cache.containsKey(md5)) return cache.get(md5) ;
		
		byte[] bases = findBasesByName(record.getSequenceName(),
				tryNameVariants);
		if (bases != null) {
			cache.put(record.getSequenceName(), bases) ;
			return bases;
		}

		if (md5 != null)
			try {
				bases = findBasesByMD5(md5);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		if (bases != null) {
			cache.put(md5, bases) ;
			return bases;
		}

		return null;
	}

	protected byte[] findBasesByName(String name, boolean tryVariants) {
		if (rsFile == null)
			return null;
		ReferenceSequence sequence = rsFile.getSequence(name);
		if (sequence != null)
			return sequence.getBases();

		if (tryVariants) {
			for (String variant : getVariants(name)) {
				sequence = rsFile.getSequence(variant);
				if (sequence != null)
					return sequence.getBases();
			}
		}
		return null;
	}

	protected byte[] findBasesByMD5(String md5) throws MalformedURLException,
			IOException {
		String url = String.format("http://www.ebi.ac.uk/ena/cram/md5/%s", md5);
		InputStream is = new URL(url).openStream();
		if (is == null)
			return null;

		log.info("Downloading reference sequence: " + url);
		return ByteBufferUtils.readFully(is);
	}

	private static final Pattern chrPattern = Pattern.compile("chr.*",
			Pattern.CASE_INSENSITIVE);

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
}
