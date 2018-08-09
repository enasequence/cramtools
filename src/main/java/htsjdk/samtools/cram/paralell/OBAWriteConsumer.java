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

package htsjdk.samtools.cram.paralell;

import htsjdk.samtools.util.RuntimeIOException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

class OBAWriteConsumer implements Consumer<OrderedByteArray> {
	private OutputStream os;

	public OBAWriteConsumer(OutputStream os) {
		this.os = os;
	}

	@Override
	public void accept(OrderedByteArray t) {
		try {
			os.write(t.bytes);
		} catch (IOException e) {
			throw new RuntimeIOException(e);
		}
	}
}
