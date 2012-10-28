/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
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
package net.sf.cram.stats;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.math.stat.Frequency;

public class LimitedBag extends HashBag {
	public static final int MAX_DISTINCT_VALUES = 1000;

	private String key;
	private HashBag bag;
	private int maxDistinctValues;

	public LimitedBag(String key, int maxDistinctValues, HashBag bag) {
		if (key == null)
			throw new NullPointerException("Key is null.");
		if (bag == null)
			throw new NullPointerException("Bag is null.");
		if (maxDistinctValues < 1)
			throw new IllegalArgumentException("Max distinct values must be more than one.");

		this.key = key;
		this.maxDistinctValues = maxDistinctValues;
		this.bag = bag;
	}

	public LimitedBag(String key, int maxDistinctValues) {
		this(key, maxDistinctValues, new HashBag());
	}

	public LimitedBag(String key) {
		this(key, MAX_DISTINCT_VALUES, new HashBag());
	}

	public boolean add(Object value) {
		if (isFull())
			return true;
		return bag.add(value);
	}

	public boolean isFull() {
		return bag.size() >= maxDistinctValues;
	}

	public String getKey() {
		return key;
	}

	public HashBag getValueBag() {
		return bag;
	}

	public int getCount(Object object) {
		return bag.getCount(object);
	}

	public boolean add(Object object, int nCopies) {
		if (isFull())
			return true;
		return add(object, nCopies);
	}

	public boolean remove(Object object) {
		return bag.remove(object);
	}

	public boolean remove(Object object, int nCopies) {
		return bag.remove(object);
	}

	public Set uniqueSet() {
		return bag.uniqueSet();
	}

	public int size() {
		return bag.size();
	}

}
