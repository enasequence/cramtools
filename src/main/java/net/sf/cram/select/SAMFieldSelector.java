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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Describes and applies selection rules to SAMRecord fields. Examples:
 * 
 * <pre>
 * /*					select all
 * *					select all
 * SEQ					select only column with bases
 * *:SEQ				select all columns including tags except for bases
 * *:SEQ:AM				select all columns except bases and tag AM
 * *:SEQ/*:AM			select all columns except bases and tag AM
 * SEQ:RNAME/*:NM:AM	select bases, ref name and all tags except for NM and AM
 * SEQ:RNAME:NM:AM		select bases, ref name and tags NM and AM
 * </pre>
 * 
 * @author vadim
 * 
 */
public class SAMFieldSelector {
	public static final String allSymbol = "*";
	public static final String fieldSeparatorSymbol = ":";
	public static final String fieldTagSeparatorSymbol = "/";
	public static final EnumSet<FIELD_TYPE> onlyTags = EnumSet.of(FIELD_TYPE.TAG);
	public static final EnumSet<FIELD_TYPE> exceptTags = EnumSet.complementOf(onlyTags);
	private static final Set<SAMRecordField> ALL_FIELDS = new TreeSet<SAMRecordField>();
	{
		for (FIELD_TYPE type : exceptTags)
			ALL_FIELDS.add(new SAMRecordField(type));
	}
	public static final Pattern pattern = Pattern
			.compile("^([*]?)([\\p{Upper}:]+)?(?:(/?)([*]?)([\\p{Upper}\\d:]+)?)?$");

	protected Set<SAMRecordField> fields = new TreeSet<SAMRecordField>();
	protected Set<SAMRecordField> tags = new TreeSet<SAMRecordField>();
	protected boolean allButFields = false;
	protected boolean allButTags = false;

	private int power = 0;
	private Map<String, SAMRecordField> tagFieldCache = new TreeMap<String, SAMRecordField>();

	public SAMFieldSelector(String spec) {
		Matcher matcher = pattern.matcher(spec);
		if (!matcher.matches() || matcher.groupCount() != 5)
			throw new IllegalArgumentException("Confusing SAMRecord field selector: " + spec);

		allButFields = allSymbol.equals(matcher.group(1));
		if (matcher.group(3).length() == 0)
			allButTags = allButFields;
		else
			allButTags = allSymbol.equals(matcher.group(4));

		Set<SAMRecordField> f1 = parseListOfFields(matcher.group(2));
		Set<SAMRecordField> f2 = parseListOfFields(matcher.group(5));

		fields.addAll(filterByType(f1, exceptTags));
		tags.addAll(filterByType(f1, onlyTags));
		tags.addAll(filterByType(f2, onlyTags));
	}

	private SAMRecordField getCachedTagField(String id) {
		SAMRecordField f = tagFieldCache.get(id);
		if (f == null) {
			f = SAMRecordField.fromTagId(id);
			tagFieldCache.put(id, f);
		}

		return f;
	}

	public Map<SAMRecordField, Object> getValues(SAMRecord record, Map<SAMRecordField, Object> map) {
		if (map == null)
			map = new TreeMap<SAMRecordField, Object>();

		if (allButFields) {
			for (SAMRecordField f : ALL_FIELDS) {
				if (!fields.contains(f))
					map.put(f, f.getValue(record));
			}
		} else {
			for (SAMRecordField f : fields) {
				if (fields.contains(f))
					map.put(f, f.getValue(record));
			}
		}

		if (allButTags) {
			for (SAMRecord.SAMTagAndValue tv : record.getAttributes()) {
				SAMRecordField f = getCachedTagField(tv.tag);
				if (!tags.contains(f))
					map.put(f, f.getValue(record));

			}
		} else {
			for (SAMRecord.SAMTagAndValue tv : record.getAttributes()) {
				SAMRecordField f = getCachedTagField(tv.tag);
				if (tags.contains(f))
					map.put(f, f.getValue(record));
			}
		}

		return map;
	}

	public boolean matches(SAMRecordField field) {
		if (field.type == FIELD_TYPE.TAG)
			return allButTags ^ tags.contains(field);

		return allButFields ^ fields.contains(field);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (allButFields && fields.isEmpty()) {
			if (allButTags && tags.isEmpty())
				return "*";
		}

		if (allButFields)
			sb.append(allSymbol);
		if (!fields.isEmpty()) {
			boolean first = true;
			for (SAMRecordField f : fields) {
				if (!first || allButFields)
					sb.append(":");
				sb.append(f.toString());
				first = false;
			}
		}

		if (allButTags || !tags.isEmpty()) {
			sb.append("/");
			if (allButTags)
				sb.append(allSymbol);
			if (!tags.isEmpty()) {
				boolean first = true;
				for (SAMRecordField f : tags) {
					if (!first || allButTags)
						sb.append(":");
					sb.append(f.toString());
					first = false;
				}
			}
		}

		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SAMFieldSelector))
			return false;
		SAMFieldSelector f = (SAMFieldSelector) obj;

		if (allButFields != f.allButFields)
			return false;
		if (allButTags != f.allButTags)
			return false;
		if (!fields.equals(f.fields))
			return false;
		if (!tags.equals(f.tags))
			return false;

		return super.equals(obj);
	}

	public static Set<SAMRecordField> filterByType(Set<SAMRecordField> set, EnumSet<FIELD_TYPE> types) {
		Set<SAMRecordField> result = new TreeSet<SAMRecordField>();
		for (SAMRecordField f : set)
			if (types.contains(f.type))
				result.add(f);
		return result;
	}

	public static Set<SAMRecordField> parseListOfFields(String spec) {
		Set<SAMRecordField> set = new TreeSet<SAMRecordField>();
		if (spec == null)
			return set;

		for (String s : spec.split(":")) {
			if (s.length() == 0)
				continue;
			SAMRecordField field = SAMRecordField.parseString(s);
			set.add(field);
		}

		return set;
	}

	/**
	 * Not sure if needed. Allows to specifiy details about each field, only [N]
	 * the length of the field is supported.
	 * 
	 * @author vadim
	 * 
	 */
	private static class FieldSpec {
		public SAMRecordField field;
		public int maxLen = Integer.MAX_VALUE;
		public String trimMarker = "...";

		private static Pattern trimPattern = Pattern.compile("^(\\p{Upper}+)(?:\\[(\\d+)\\])?$");

		public FieldSpec(String spec) {
			// handles only [] for now:
			Matcher matcher = trimPattern.matcher(spec);
			if (matcher.matches()) {
				switch (matcher.groupCount()) {
				case 1:
					field = SAMRecordField.parseString(matcher.group(1));
					break;
				case 2:
					field = SAMRecordField.parseString(matcher.group(1));
					if (matcher.group(2) != null)
						maxLen = Integer.parseInt(matcher.group(2));
					break;

				default:
					throw new IllegalArgumentException("Failed to parse field specification: " + spec);
				}
			}
		}

		@Override
		public String toString() {
			if (maxLen < Integer.MAX_VALUE)
				return String.format("%s[%d]", field.toString(), maxLen);

			return field.toString();
		}
	}

	public static void main(String[] args) {
		test("SEQ");
		test("*:SEQ");
		test("*:SEQ:AM");
		test("*SEQ:AM");
		test("*:SEQ/*:AM");
		test("SEQ:RNAME/*:NM:AM");
		test("SEQ:RNAME:NM:AM");
		test("*/*");
		test("*");

		SAMFieldSelector s = new SAMFieldSelector("*:SEQ:RNAME:NM:AM");
		System.out.println(s);
		System.out.println(s.matches(new SAMRecordField(FIELD_TYPE.RNAME)));
		System.out.println(s.matches(new SAMRecordField(FIELD_TYPE.SEQ)));
		System.out.println(s.matches(new SAMRecordField(FIELD_TYPE.POS)));

		System.out.println(s.matches(SAMRecordField.fromTagId("NM")));
		System.out.println(s.matches(SAMRecordField.fromTagId("AM")));
		System.out.println(s.matches(SAMRecordField.fromTagId("OQ")));

	}

	private static void test(String spec) {
		SAMFieldSelector s = new SAMFieldSelector(spec);
		System.out.println(spec + "\t" + s.toString());
		System.out.println(new SAMFieldSelector(s.toString()).toString());
		System.out.println();
	}
}
