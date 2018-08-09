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

package htsjdk.samtools.cram.structure;

import java.io.IOException;
import java.io.InputStream;

public class SliceIO_Accessor {

	public static void readIntoSliceFromStream(final int major, final Slice slice, final InputStream inputStream) throws IOException {
		SliceIO.read(major, slice, inputStream);
	}

}
