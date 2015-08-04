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
package net.sf.cram.select;

import htsjdk.samtools.SAMRecord;

import java.util.Arrays;


public class SAMRecordField implements Comparable<SAMRecordField> {
	public static SAMRecordField[] SHARED = new SAMRecordField[FIELD_TYPE.values().length];
	static {
		for (FIELD_TYPE type : FIELD_TYPE.values())
			SHARED[type.ordinal()] = new SAMRecordField(type);
	}
	public FIELD_TYPE type;
	public String tagId;

	public SAMRecordField(FIELD_TYPE type, String tagId) {
		if (type == null)
			throw new IllegalArgumentException("Record field type not specified (null).");

		this.type = type;
		this.tagId = tagId;
	}

	public SAMRecordField(FIELD_TYPE type) {
		this(type, null);
	}

	@Override
	public String toString() {
		if (type == FIELD_TYPE.TAG)
			return tagId;
		return type.name();
	}

	public static String toString(Object value) {
		if (value == null)
			return null;
		if (value.getClass().isArray()) {

			if (value instanceof long[])
				return Arrays.toString((long[]) value);

			if (value instanceof int[])
				return Arrays.toString((int[]) value);

			if (value instanceof short[])
				return Arrays.toString((short[]) value);

			if (value instanceof byte[])
				return new String((byte[]) value);

			// this will fail if it is a primitive array:
			return Arrays.toString((Object[]) value);
		} else
			return value.toString();
	}

	public static SAMRecordField fromTagId(String id) {
		return new SAMRecordField(FIELD_TYPE.TAG, id);
	}

	public static SAMRecordField parseString(String spec) {
		try {
			// try to treat the string as a field name:
			FIELD_TYPE type = FIELD_TYPE.valueOf(spec);
			return new SAMRecordField(type);
		} catch (Exception e) {
			// assume this is a 2-byte tag id:
			if (spec.length() == 2)
				return new SAMRecordField(FIELD_TYPE.TAG, spec);
		}

		throw new IllegalArgumentException("The specificator is neither field name nor 2 char tag id: " + spec);
	}

	@Override
	public int compareTo(SAMRecordField o) {
		if (type != o.type)
			return type.ordinal() - o.type.ordinal();

		if (type == FIELD_TYPE.TAG)
			return tagId.compareTo(o.tagId);

		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof SAMRecordField))
			return false;
		SAMRecordField f = (SAMRecordField) o;

		return compareTo(f) == 0;
	}

	public Object getValue(SAMRecord record) {
		switch (type) {
		case QNAME:
			return record.getReadName();
		case FLAG:
			return Integer.toString(record.getFlags());
		case RNAME:
			return record.getReferenceName();
		case POS:
			return Integer.toString(record.getAlignmentStart());
		case MAPQ:
			return Integer.toString(record.getMappingQuality());
		case CIGAR:
			return record.getCigarString();
		case RNEXT:
			return record.getMateReferenceName();
		case PNEXT:
			return Integer.toString(record.getMateAlignmentStart());
		case TLEN:
			return Integer.toString(record.getInferredInsertSize());
		case SEQ:
			return record.getReadString();
		case QUAL:
			return record.getBaseQualityString();

		case TAG:
			if (tagId == null)
				throw new IllegalArgumentException("Tag mismatch reqiues tag id. ");
			return record.getAttribute(tagId);

		default:
			throw new IllegalArgumentException("Unknown record field: " + type.name());
		}
	}
}
