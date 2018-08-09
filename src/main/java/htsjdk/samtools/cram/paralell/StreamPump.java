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

import htsjdk.samtools.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class StreamPump extends Job {
	private static final Log log = Log.getInstance(StreamPump.class);
	InputStream is;
	OutputStream os;

	public StreamPump(InputStream is, OutputStream os) {
		this.is = is;
		this.os = os;
	}

	@Override
	protected void doRun() throws IOException {
		byte[] buf = new byte[4096];
		int len = is.read(buf);
		if (len == -1) {
			stop();
		} else {
			os.write(buf, 0, len);
		}
	}

	@Override
	protected void doFinish() throws Exception {
		log.info("stream pump out");
		os.close();
	}

}