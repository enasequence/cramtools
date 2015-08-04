package htsjdk.samtools.cram.encoding.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import htsjdk.samtools.cram.encoding.reader.NameCollate.IRead;
import htsjdk.samtools.util.Log;

public abstract class NameCollate<R extends IRead> {
	private static final Log log = Log.getInstance(NameCollate.class);
	private Map<R, R> readSet = new TreeMap<R, R>();
	private int maxCacheSize = 100000;
	private long generation = 0;

	protected void foundCollision(R key, R alien) {
		R anchor = readSet.remove(key);
		ready(anchor);
		ready(alien);
	}

	private Comparator<R> byGenerationComparator = new Comparator<R>() {

		@Override
		public int compare(R o1, R o2) {
			return (int) (o1.getAge() - o2.getAge());
		}
	};

	private List<R> list = new ArrayList<R>();

	protected void purgeCache() {
		long time1 = System.nanoTime();
		list.clear();
		for (R read : readSet.keySet())
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

	protected abstract boolean needsCollating(R read);

	protected abstract void ready(R read);

	protected abstract void kickedFromCache(R read);

	public void add(R read) {
		read.setAge(generation++);

		if (!needsCollating(read)) {
			ready(read);
			return;
		}

		if (readSet.containsKey(read)) {
			foundCollision(readSet.get(read), read);
		} else {
			readSet.put(read, read);

			if (readSet.size() > maxCacheSize)
				purgeCache();
		}
	}

	public void close() {
		for (R read : readSet.keySet()) {
			kickedFromCache(read);
		}
		readSet.clear();
	}

	public interface IRead extends Comparable<IRead> {
		public long getAge();

		public void setAge(long age);
	}
}
