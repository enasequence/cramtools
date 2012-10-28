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

import java.util.Map;
import java.util.TreeMap;

import uk.ac.ebi.ena.sra.cram.format.ReadTag;

public class ReadTagStats {

	private Map<String, LimitedBag> tagBags = new TreeMap<String, LimitedBag>();

	public void add(ReadTag tag) {
		if (!tagBags.containsKey(tag.getKey()))
			tagBags.put(tag.getKey(), new LimitedBag(tag.getKey()));

		tagBags.get(tag.getKey()).add(tag.getValue());
	}
}
