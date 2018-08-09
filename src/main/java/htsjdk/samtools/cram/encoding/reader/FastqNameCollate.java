/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package htsjdk.samtools.cram.encoding.reader;

import java.io.IOException;
import java.io.OutputStream;

public class FastqNameCollate extends NameCollate<FastqRead> {
	private OutputStream[] streams;
	private OutputStream overspillStream;
	private long kicked = 0, ready = 0, total = 0;

	public FastqNameCollate(OutputStream[] streams, OutputStream overspillStream) {
		super();
		this.streams = streams;
		this.overspillStream = overspillStream;
	}

	@Override
	protected boolean needsCollating(FastqRead read) {
		return read.templateIndex > 0;
	}

	@Override
	protected void ready(FastqRead read) {
		try {
			streams[read.templateIndex].write(read.data);
			ready++;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void kickedFromCache(FastqRead read) {
		try {
			overspillStream.write(read.data);
			kicked++;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void add(FastqRead read) {
		super.add(read);
		total++;
	}

	public String report() {
		return String.format("ready: %d, kicked %d, total: %d.", ready, kicked, total);
	}
}
