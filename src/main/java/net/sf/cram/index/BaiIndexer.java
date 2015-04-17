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
package net.sf.cram.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import net.sf.cram.build.CramIO;
import net.sf.cram.io.CountingInputStream;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.CramHeader;
import net.sf.cram.structure.Slice;
import net.sf.picard.util.Log;
import net.sf.samtools.CRAMIndexer;
import net.sf.samtools.SAMFileHeader;

class BaiIndexer {
	private static Log log = Log.getInstance(BaiIndexer.class);

	public CountingInputStream is;
	public SAMFileHeader samFileHeader;
	public CRAMIndexer indexer;

	private CramHeader cramHeader;

	public BaiIndexer(InputStream is, File output) throws IOException {
		this.is = new CountingInputStream(is);
		cramHeader = CramIO.readCramHeader(this.is);
		samFileHeader = cramHeader.samFileHeader;

		indexer = new CRAMIndexer(output, samFileHeader);
	}

	private boolean nextContainer() throws IOException {
		long offset = is.getCount();
		Container c = CramIO.readContainer(cramHeader, is);
		if (c == null || c.isEOF())
			return false;
		c.offset = offset;

		int i = 0;
		for (Slice slice : c.slices) {
			slice.containerOffset = offset;
			slice.index = i++;
			indexer.processAlignment(slice);
		}

		log.info("INDEXED: " + c.toString());
		return true;
	}

	private void index() throws IOException {
		while (true) {
			if (!nextContainer())
				break;
		}
	}

	public void run() throws IOException {
		index();
		indexer.finish();
	}
}
