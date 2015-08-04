package net.sf.cram.cg;

import htsjdk.samtools.util.CloseableIterator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;


class DedupIterator implements CloseableIterator<EvidenceRecord> {
	private static final int DEFAULT_MAX_CACHE = 10000;
	private int cacheSize;
	private EvidenceRecordFileIterator it;
	private Map<String, EvidenceRecord> cache = new HashMap<String, EvidenceRecord>();
	private PriorityQueue<EvidenceRecord> queue;

	DedupIterator(EvidenceRecordFileIterator it, int cacheSize) {
		this.it = it;
		this.cacheSize = cacheSize;
		queue = new PriorityQueue<EvidenceRecord>(cacheSize, new Comparator<EvidenceRecord>() {

			@Override
			public int compare(EvidenceRecord o1, EvidenceRecord o2) {
				return o1.pos - o2.pos;
			}
		});
	}

	DedupIterator(EvidenceRecordFileIterator it) {
		this(it, DEFAULT_MAX_CACHE);
	}

	@Override
	public boolean hasNext() {
		while (queue.size() < cacheSize && it.hasNext()) {
			EvidenceRecord evidenceRecord = it.next();
			String name = evidenceRecord.getReadName();
			if (cache.containsKey(name)) {
				if (!queue.remove(cache.get(name)))
					throw new RuntimeException("Invalid cache state: element not in the queue.");
				evidenceRecord = dedup(cache.get(name), evidenceRecord);
				queue.add(evidenceRecord);
				cache.put(name, evidenceRecord);
			} else {
				queue.add(evidenceRecord);
				cache.put(name, evidenceRecord);
			}
		}
		return !queue.isEmpty();
	}

	private EvidenceRecord dedup(EvidenceRecord e1, EvidenceRecord e2) {
		if (e2.mapq > e1.mapq)
			return e2;
		return e1;
	}

	@Override
	public EvidenceRecord next() {
		EvidenceRecord first = queue.remove();
		if (null == cache.remove(first.name))
			throw new RuntimeException("Invalid cache state: element not in the map.");
		return first;
	}

	@Override
	public void remove() {
		it.remove();
	}

	@Override
	public void close() {
		it.close();
	}

}
