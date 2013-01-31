package net.sf.samtools;

import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SAMSelector {
	public enum FIELD_TYPE {
		QNAME, FLAG, RNAME, POS, MAPQ, CIGAR, RNEXT, PNEXT, TLEN, SEQ, QUAL, TAG;
	}

	public static class Field implements Comparable<Field> {
		FIELD_TYPE type;
		String tagId;

		public Field(FIELD_TYPE type, String tagId) {
			if (type == null)
				throw new IllegalArgumentException(
						"Record field type not specified (null).");

			this.type = type;
			this.tagId = tagId;
		}

		public Field(FIELD_TYPE type) {
			this(type, null);
		}

		@Override
		public String toString() {
			if (type == FIELD_TYPE.TAG)
				return tagId;
			return type.name();
		}

		public static Field fromTagId(String id) {
			return new Field(FIELD_TYPE.TAG, id);
		}

		public static Field parseString(String spec) {
			try {
				// try to treat the string as a field name:
				FIELD_TYPE type = FIELD_TYPE.valueOf(spec);
				return new Field(type);
			} catch (Exception e) {
				// assume this is a 2-byte tag id:
				if (spec.length() == 2)
					return new Field(FIELD_TYPE.TAG, spec);
			}

			throw new IllegalArgumentException(
					"The specificator is neither field name nor 2 char tag id: "
							+ spec);
		}

		@Override
		public int compareTo(Field o) {
			if (type != o.type)
				return type.ordinal() - o.type.ordinal();

			if (type == FIELD_TYPE.TAG)
				return tagId.compareTo(o.tagId);

			return 0;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Field))
				return false;
			Field f = (Field) o;

			return compareTo(f) == 0;
		}
	}

	public static class FieldSpec {
		public Field field;
		public int maxLen = Integer.MAX_VALUE;
		public String trimMarker = "...";

		private static Pattern trimPattern = Pattern
				.compile("^(\\p{Upper}+)(?:\\[(\\d+)\\])?$");

		public FieldSpec(String spec) {
			// handles only [] for now:
			Matcher matcher = trimPattern.matcher(spec);
			if (matcher.matches()) {
				switch (matcher.groupCount()) {
				case 1:
					field = Field.parseString(matcher.group(1));
					break;
				case 2:
					field = Field.parseString(matcher.group(1));
					if (matcher.group(2) != null)
						maxLen = Integer.parseInt(matcher.group(2));
					break;

				default:
					throw new IllegalArgumentException(
							"Failed to parse field specification: " + spec);
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

	// public EnumSet<FIELD_TYPE> fields = EnumSet.allOf(FIELD_TYPE.class);
	public Set<Field> fields = new TreeSet<SAMSelector.Field>();
	public Set<Field> tags = new TreeSet<SAMSelector.Field>();
	public boolean allButFields = false;
	public boolean allButTags = false;

	public static final String allSymbol = "*";
	public static final String fieldSeparatorSymbol = ":";
	public static final String fieldTagSeparatorSymbol = "/";
	public static final EnumSet<FIELD_TYPE> onlyTags = EnumSet
			.of(FIELD_TYPE.TAG);
	public static final EnumSet<FIELD_TYPE> exceptTags = EnumSet
			.complementOf(onlyTags);

	/**
	 * QNAME:POS:SEQ[10]:AM:NM:OQ[10] MD:Z. QNAME:POS/NM-:MD-. *\/*
	 * +:SEQ:QUAL/-:AM -/OQ, OQ *
	 * 
	 * @param spec
	 */
	public SAMSelector(String spec) {
		Pattern pattern = Pattern
				.compile("^([*]?)([\\p{Upper}:]+)?(?:(/?)([*]?)([\\p{Upper}:]+)?)?$");
		Matcher matcher = pattern.matcher(spec);
		if (!matcher.matches() || matcher.groupCount() != 5)
			throw new IllegalArgumentException(
					"Confusing SAMRecord field selector: " + spec);

		allButFields = allSymbol.equals(matcher.group(1));
		if (matcher.group(3).length() == 0)
			allButTags = allButFields;
		else
			allButTags = allSymbol.equals(matcher.group(4));

		Set<Field> f1 = parseListOfFields(matcher.group(2));
		Set<Field> f2 = parseListOfFields(matcher.group(5));

		fields.addAll(filterByType(f1, exceptTags));
		tags.addAll(filterByType(f1, onlyTags));
		tags.addAll(filterByType(f2, onlyTags));
	}

	public boolean matches(Field field) {
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
			for (Field f : fields) {
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
				for (Field f : tags) {
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
		if (!(obj instanceof SAMSelector))
			return false;
		SAMSelector f = (SAMSelector) obj;

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

	public static Set<Field> filterByType(Set<Field> set,
			EnumSet<FIELD_TYPE> types) {
		Set<Field> result = new TreeSet<SAMSelector.Field>();
		for (Field f : set)
			if (types.contains(f.type))
				result.add(f);
		return result;
	}

	public static Set<Field> parseListOfFields(String spec) {
		Set<Field> set = new TreeSet<SAMSelector.Field>();
		if (spec == null)
			return set;

		for (String s : spec.split(":")) {
			if (s.length() == 0)
				continue;
			Field field = Field.parseString(s);
			set.add(field);
		}

		return set;
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

		SAMSelector s = new SAMSelector("*:SEQ:RNAME:NM:AM");
		System.out.println(s);
		System.out.println(s.matches(new Field(FIELD_TYPE.RNAME)));
		System.out.println(s.matches(new Field(FIELD_TYPE.SEQ)));
		System.out.println(s.matches(new Field(FIELD_TYPE.POS)));

		System.out.println(s.matches(Field.fromTagId("NM")));
		System.out.println(s.matches(Field.fromTagId("AM")));
		System.out.println(s.matches(Field.fromTagId("OQ")));

	}

	private static void test(String spec) {
		SAMSelector s = new SAMSelector(spec);
		System.out.println(spec + "\t" + s.toString());
		System.out.println(new SAMSelector(s.toString()).toString());
		System.out.println();
	}
}
