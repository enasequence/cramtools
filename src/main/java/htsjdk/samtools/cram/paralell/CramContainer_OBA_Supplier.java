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

import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.RuntimeIOException;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

class CramContainer_OBA_Supplier implements Supplier<OrderedByteArray> {
	private static final int MAX_CONTAINER_HEADER_BYTESIZE = 1024 * 10;
	private Log log = Log.getInstance(CramContainer_OBA_Supplier.class);
	private DataInputStream is;
	private long order = 0;
	private CramHeader cramHeader;

	public CramContainer_OBA_Supplier(InputStream is, CramHeader cramHeader) {
		if (is.markSupported())
			this.is = new DataInputStream(is);
		else
			this.is = new DataInputStream(new BufferedInputStream(is, MAX_CONTAINER_HEADER_BYTESIZE));
		this.cramHeader = cramHeader;
	}

	@Override
	public OrderedByteArray get() {
		OrderedByteArray cb = new OrderedByteArray();
		Container containerHeader;
		try {
			containerHeader = ContainerIO.readContainerHeader(cramHeader.getVersion().major, is);
			log.debug("Read container: " + containerHeader.toString());
			if (containerHeader.isEOF()) {
				log.info("EOF container");
				return null;
			}

			ByteArrayOutputStream baos = new ByteArrayOutputStream(containerHeader.containerByteSize + 1024 * 10);

			ContainerIO.writeContainerHeader(cramHeader.getVersion().major, containerHeader, baos);
			byte[] blocks = InputStreamUtils.readFully(is, containerHeader.containerByteSize);
			baos.write(blocks);

			cb.bytes = baos.toByteArray();
			cb.order = order++;
			return cb;
		} catch (IOException e) {
			throw new RuntimeIOException(e);
		}
	}
}