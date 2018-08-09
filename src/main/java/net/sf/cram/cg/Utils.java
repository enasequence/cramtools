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

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class Utils {
	static List<CigEl> parseCigarInto(String cigar, List<CigEl> list) {
		int len = 0;
		for (int i = 0; i < cigar.length(); i++) {
			char ch = cigar.charAt(i);
			if (Character.isDigit(ch))
				len = len * 10 + Character.getNumericValue(ch);
			else {
				list.add(new CigEl(len, ch));
				len = 0;
			}
		}

		return list;
	}

	static byte[] toByteArray(ByteBuffer buf) {
		buf.rewind();
		byte[] bytes = new byte[buf.limit()];
		buf.get(bytes);
		return bytes;
	}

	public static String toString(ByteBuffer buf) {
		int pos = buf.position();
		buf.rewind();
		byte[] bytes = new byte[buf.limit()];
		buf.get(bytes);
		buf.position(pos);
		return new String(bytes);
	}

	public static ByteBuffer slice(ByteBuffer buf, int pos, int limit) {
		int oldPos = buf.position();
		buf.position(pos);
		ByteBuffer result = buf.slice();
		result.limit(limit - pos);

		buf.position(oldPos);
		return result;
	}

	public static void printCgList(List<CigEl> list) {
		System.out.println();
		for (CigEl e : list) {
			System.out.println(e);
		}
	}

	public static String toString(List<CigEl> list) {
		StringBuilder sb = new StringBuilder();
		for (CigEl e : list)
			sb.append(Integer.toString(e.len)).append(e.op);
		return sb.toString();
	}

	public static List<CigarElement> toCigarOperatorList(List<CigEl> list) {
		List<CigarElement> result = new ArrayList<CigarElement>(list.size());
		for (CigEl e : list)
			result.add(new CigarElement(e.len, CigarOperator.characterToEnum(e.op)));
		return result;
	}
}
