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

package net.sf.cram.cg;

import java.nio.ByteBuffer;

class CigEl {
	int len;
	char op;
	ByteBuffer bases, scores;

	CigEl() {
	}

	CigEl(int len, char op) {
		this.len = len;
		this.op = op;
	}

	@Override
	public String toString() {
		if (bases != null) {
			bases.rewind();
			byte[] bytes = new byte[bases.limit()];
			bases.get(bytes);
			return String.format("%c, %d, %s", op, len, new String(bytes));
		} else
			return String.format("%c, %d, no bases", op, len);
	}
}