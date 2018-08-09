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

package net.sf.cram;

import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Scans a cram file and exits with exit code 1 if a multiref slice is found.
 * 
 * @author vadim
 *
 */
public class DetectMultiref {

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		Log.setGlobalLogLevel(LogLevel.INFO);
		File cramFile = new File(args[0]);
		InputStream is = new BufferedInputStream(new FileInputStream(cramFile));
		CramHeader header = CramIO.readCramHeader(is);
		Container c = null;
		while ((c = ContainerIO.readContainer(header.getVersion(), is)) != null && !c.isEOF()) {
			for (Slice slice : c.slices) {
				if (slice.sequenceId == Slice.MULTI_REFERENCE) {
					System.out.println("Read feature B detected.");
					System.exit(1);
				}
			}
		}
	}
}
