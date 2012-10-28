/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
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
package net.sf.cram;

import net.sf.samtools.SAMTagUtil;

public class ReadTag implements Comparable<ReadTag> {
	private static final long MAX_INT = Integer.MAX_VALUE;
	private static final long MAX_UINT = MAX_INT * 2 + 1;
	private static final long MAX_SHORT = Short.MAX_VALUE;
	private static final long MAX_USHORT = MAX_SHORT * 2 + 1;
	private static final long MAX_BYTE = Byte.MAX_VALUE;
	private static final long MAX_UBYTE = MAX_BYTE * 2 + 1;

	// non-null
	private String key;
	private String keyAndType;
	private char type;
	private Object value;
	public short code ;
	private byte index;

	public ReadTag(String key, char type, Object value) {
		if (key == null)
			throw new NullPointerException("Tag key cannot be null.");
		if (value == null)
			throw new NullPointerException("Tag value cannot be null.");

		this.value = value;

		if (key.length() == 2) {
			this.key = key;
			this.type = type;
			// this.type = getTagValueType(value);
			keyAndType = key + ":" + getType();
		} else if (key.length() == 4) {
			this.key = key.substring(0, 2);
			this.type = key.charAt(3);
		}
		
		code = SAMTagUtil.getSingleton().makeBinaryTag(this.key) ;
	}

	public static ReadTag deriveTypeFromKeyAndType(String keyAndType, Object value) {
		if (keyAndType.length() != 4)
			throw new RuntimeException("Tag key and type must be 4 char long: " + keyAndType);

		return new ReadTag(keyAndType.substring(0, 2), keyAndType.charAt(3), value);
	}

	public static ReadTag deriveTypeFromValue(String key, Object value) {
		if (key.length() != 2)
			throw new RuntimeException("Tag key must be 2 char long: " + key);

		return new ReadTag(key, getTagValueType(value), value);
	}

	public String getKey() {
		return key;
	}

	@Override
	public int compareTo(ReadTag o) {
		return key.compareTo(o.key);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ReadTag))
			return false;

		ReadTag foe = (ReadTag) obj;
		if (!key.equals(foe.key))
			return false;
		if (value == null && foe.value == null)
			return true;
		if (value != null && value.equals(foe.value))
			return true;

		return false;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	public Object getValue() {
		return value;
	}

	public char getType() {
		return type;
	}

	public String getKeyAndType() {
		return keyAndType;
	}

	public byte[] getValueAsByteArray() {
		if (value.getClass().isArray()) {
			if (value instanceof byte[])
				return toByteArray((byte[]) value);
			if (value instanceof short[])
				return toByteArray((short[]) value);
			if (value instanceof int[])
				return toByteArray((int[]) value);

			throw new RuntimeException("Unsupported tag array type.");
		}
		return value.toString().getBytes();
	}

	private static byte[] toByteArray(byte[] value) {
		byte[] array = new byte[value.length + 1];
		array[0] = 'c';
		System.arraycopy(value, 0, array, 1, value.length);
		return array;
	}

	private static Object fromByteArray(byte[] array, int offset, int length) {
		byte first = array[offset];
		switch (first) {
		case 'c':
			byte[] byteArrayValue = new byte[length - 1];
			System.arraycopy(array, 1 + offset, byteArrayValue, 0, byteArrayValue.length);
			return byteArrayValue;
		case 's':
			short[] shortArrayValue = new short[(length - 1) / 2];
			int p = offset + 1;
			for (int i = 0; i < shortArrayValue.length; i++) {
				shortArrayValue[i] = (short) (0xFFFF & ((array[p++] << 8) | array[p++]));
			}
			return shortArrayValue;
		case 'i':
			int[] intArrayValue = new int[(length - 1) / 4];
			p = offset + 1;
			for (int i = 0; i < intArrayValue.length; i++) {
				intArrayValue[i] = (array[p++] << 24) | (array[p++] << 16) | (array[p++] << 8) | array[p++];
			}
			return intArrayValue;

		default:
			throw new RuntimeException("Unknown array type in tag: " + (char) first);
		}
	}

	private static byte[] toByteArray(short[] value) {
		byte[] array = new byte[2 * value.length + 1];
		array[0] = 's';
		int i = 1;
		for (short s : value) {
			array[i++] = (byte) (0xFF & (s >> 8));
			array[i++] = (byte) (0xFF & s);
		}
		return array;
	}

	private static byte[] toByteArray(int[] value) {
		byte[] array = new byte[4 * value.length + 1];
		array[0] = 'i';
		int i = 1;
		for (int s : value) {
			array[i++] = (byte) (0xFF & (s >> 24));
			array[i++] = (byte) (0xFF & (s >> 16));
			array[i++] = (byte) (0xFF & (s >> 8));
			array[i++] = (byte) (0xFF & s);
		}
		return array;
	}

	public static Object restoreValueFromByteArray(char type, byte[] array) {
		return restoreValueFromByteArray(type, array, 0, array.length);
	}

	public static Object restoreValueFromByteArray(char type, byte[] array, int offset, int length) {
		switch (type) {
		case 'Z':
			return new String(array, offset, length);
		case 'A':
			return new String(array, offset, length).charAt(0);
		case 'f':
			return Float.valueOf(new String(array, offset, length));

		case 'I':
			return Long.valueOf(new String(array, offset, length));
		case 'i':
		case 'S':
			return Integer.valueOf(new String(array, offset, length));
		case 's':
			return Short.valueOf(new String(array, offset, length));
		case 'C':
			return Integer.valueOf(new String(array, offset, length));
		case 'c':
			return Byte.valueOf(new String(array, offset, length));
		case 'B':
			return fromByteArray(array, offset, length);

		default:
			throw new RuntimeException("Unknown tag type: " + type);
		}
	}

	// copied from net.sf.samtools.BinaryTagCodec 1.62:
	private static char getTagValueType(final Object value) {
		if (value instanceof String) {
			return 'Z';
		} else if (value instanceof Character) {
			return 'A';
		} else if (value instanceof Float) {
			return 'f';
		} else if (value instanceof Number) {
			if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)) {
				throw new IllegalArgumentException("Unrecognized tag type " + value.getClass().getName());
			}
			return getIntegerType(((Number) value).longValue());
		} /*
		 * Note that H tag type is never written anymore, because B style is
		 * more compact. else if (value instanceof byte[]) { return 'H'; }
		 */
		else if (value instanceof byte[] || value instanceof short[] || value instanceof int[]
				|| value instanceof float[]) {
			return 'B';
		} else {
			throw new IllegalArgumentException("When writing BAM, unrecognized tag type " + value.getClass().getName());
		}
	}

	// copied from net.sf.samtools.BinaryTagCodec:
	static private char getIntegerType(final long val) {
		if (val > MAX_UINT) {
			throw new IllegalArgumentException("Integer attribute value too large to be encoded in BAM");
		}
		if (val > MAX_INT) {
			return 'I';
		}
		if (val > MAX_USHORT) {
			return 'i';
		}
		if (val > MAX_SHORT) {
			return 'S';
		}
		if (val > MAX_UBYTE) {
			return 's';
		}
		if (val > MAX_BYTE) {
			return 'C';
		}
		if (val >= Byte.MIN_VALUE) {
			return 'c';
		}
		if (val >= Short.MIN_VALUE) {
			return 's';
		}
		if (val >= Integer.MIN_VALUE) {
			return 'i';
		}
		throw new IllegalArgumentException("Integer attribute value too negative to be encoded in BAM");
	}

	public void setIndex(byte i) {
		this.index = i ;
	}
	
	public byte getIndex () {
		return index ;
	}

}
