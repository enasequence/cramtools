package htsjdk.samtools.cram.structure;

import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SAMTagUtil;
import htsjdk.samtools.TagValueAndUnsignedArrayFlag;
import htsjdk.samtools.SAMRecord.SAMTagAndValue;
import htsjdk.samtools.util.StringUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class ReadTag implements Comparable<ReadTag> {
    private static final long MAX_INT = 2147483647L;
    private static final long MAX_UINT = 4294967295L;
    private static final long MAX_SHORT = 32767L;
    private static final long MAX_USHORT = 65535L;
    private static final long MAX_BYTE = 127L;
    private static final long MAX_UBYTE = 255L;
    private String key;
    private String keyAndType;
    public String keyType3Bytes;
    public int keyType3BytesAsInt;
    private char type;
    private Object value;
    private short code;
    private byte index;
    private static final ThreadLocal<ByteBuffer> bufferLocal = new ThreadLocal() {
        protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(10485760);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            return buf;
        }
    };
    private static final Charset charset = Charset.forName("US-ASCII");

    public ReadTag(int id, byte[] dataAsByteArray) {
        this.type = (char)(255 & id);
        this.key = new String(new char[]{(char)(id >> 16 & 255), (char)(id >> 8 & 255)});
        this.value = restoreValueFromByteArray(this.type, dataAsByteArray);
        this.keyType3Bytes = this.key + this.type;
        this.keyType3BytesAsInt = id;
        this.code = SAMTagUtil.getSingleton().makeBinaryTag(this.key);
    }

    private ReadTag(String key, char type, Object value) {
        if(key == null) {
            throw new NullPointerException("Tag key cannot be null.");
        } else if(value == null) {
            throw new NullPointerException("Tag value cannot be null.");
        } else {
            this.value = value;
            if(key.length() == 2) {
                this.key = key;
                this.type = type;
                this.keyAndType = key + ":" + this.getType();
            } else if(key.length() == 4) {
                this.key = key.substring(0, 2);
                this.type = key.charAt(3);
            }

            this.keyType3Bytes = this.key + this.type;
            this.keyType3BytesAsInt = nameType3BytesToInt(this.key, this.type);
            this.code = SAMTagUtil.getSingleton().makeBinaryTag(this.key);
        }
    }

    public static int name3BytesToInt(byte[] name) {
        int value = 255 & name[0];
        value <<= 8;
        value |= 255 & name[1];
        value <<= 8;
        value |= 255 & name[2];
        return value;
    }

    public static int nameType3BytesToInt(String name, char type) {
        int value = 255 & name.charAt(0);
        value <<= 8;
        value |= 255 & name.charAt(1);
        value <<= 8;
        value |= 255 & type;
        return value;
    }

    public static String intToNameType3Bytes(int value) {
        byte b3 = (byte)(255 & value);
        byte b2 = (byte)(255 & value >> 8);
        byte b1 = (byte)(255 & value >> 16);
        return new String(new byte[]{b1, b2, b3});
    }

    public static String intToNameType4Bytes(int value) {
        byte b3 = (byte)(255 & value);
        byte b2 = (byte)(255 & value >> 8);
        byte b1 = (byte)(255 & value >> 16);
        return new String(new byte[]{b1, b2, (byte)58, b3});
    }

    public SAMTagAndValue createSAMTag() {
        return new SAMTagAndValue(this.key, this.value);
    }

    public static ReadTag deriveTypeFromKeyAndType(String keyAndType, Object value) {
        if(keyAndType.length() != 4) {
            throw new RuntimeException("Tag key and type must be 4 char long: " + keyAndType);
        } else {
            return new ReadTag(keyAndType.substring(0, 2), keyAndType.charAt(3), value);
        }
    }

    public static ReadTag deriveTypeFromValue(String key, Object value) {
        if(key.length() != 2) {
            throw new RuntimeException("Tag key must be 2 char long: " + key);
        } else {
            return new ReadTag(key, getTagValueType(value), value);
        }
    }

    public String getKey() {
        return this.key;
    }

    public int compareTo(ReadTag o) {
        return this.key.compareTo(o.key);
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof ReadTag)) {
            return false;
        } else {
            ReadTag foe = (ReadTag)obj;
            return this.key.equals(foe.key) && (this.value == null && foe.value == null || this.value != null && this.value.equals(foe.value));
        }
    }

    public int hashCode() {
        return this.key.hashCode();
    }

    public Object getValue() {
        return this.value;
    }

    char getType() {
        return this.type;
    }

    public String getKeyAndType() {
        return this.keyAndType;
    }

    public byte[] getValueAsByteArray() {
        return writeSingleValue((byte)this.type, this.value, false);
    }

    private static Object restoreValueFromByteArray(char type, byte[] array) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return readSingleValue((byte)type, buffer);
    }

    public static char getTagValueType(Object value) {
        if(value instanceof String) {
            return 'Z';
        } else if(value instanceof Character) {
            return 'A';
        } else if(value instanceof Float) {
            return 'f';
        } else if(value instanceof Number) {
            if(!(value instanceof Byte) && !(value instanceof Short) && !(value instanceof Integer) && !(value instanceof Long)) {
                throw new IllegalArgumentException("Unrecognized tag type " + value.getClass().getName());
            } else {
                return getIntegerType(((Number)value).longValue());
            }
        } else if(!(value instanceof byte[]) && !(value instanceof short[]) && !(value instanceof int[]) && !(value instanceof float[])) {
            throw new IllegalArgumentException("When writing BAM, unrecognized tag type " + value.getClass().getName());
        } else {
            return 'B';
        }
    }

    private static char getIntegerType(long val) {
        if(val > 4294967295L) {
            throw new IllegalArgumentException("Integer attribute value too large to be encoded in BAM");
        } else if(val > 2147483647L) {
            return 'I';
        } else if(val > 65535L) {
            return 'i';
        } else if(val > 32767L) {
            return 'S';
        } else if(val > 255L) {
            return 's';
        } else if(val > 127L) {
            return 'C';
        } else if(val >= -128L) {
            return 'c';
        } else if(val >= -32768L) {
            return 's';
        } else if(val >= -2147483648L) {
            return 'i';
        } else {
            throw new IllegalArgumentException("Integer attribute value too negative to be encoded in BAM");
        }
    }

    public void setIndex(byte i) {
        this.index = i;
    }

    public byte getIndex() {
        return this.index;
    }

    public static byte[] writeSingleValue(byte tagType, Object value, boolean isUnsignedArray) {
        ByteBuffer buffer = (ByteBuffer)bufferLocal.get();
        buffer.clear();
        String bytes;
        switch(tagType) {
            case 65:
                buffer.put((byte)((Character)value).charValue());
                break;
            case 66:
                writeArray(value, isUnsignedArray, buffer);
                break;
            case 67:
                buffer.putShort(((Integer)value).shortValue());
                buffer.position(buffer.position() - 1);
                break;
            case 72:
                bytes = StringUtil.bytesToHexString((byte[])((byte[])value));
                buffer.put(bytes.getBytes(charset));
                buffer.put((byte)0);
                break;
            case 73:
                buffer.putLong(((Long)value).longValue());
                buffer.position(buffer.position() - 4);
                break;
            case 83:
                buffer.putInt(((Number)value).intValue());
                buffer.position(buffer.position() - 2);
                break;
            case 90:
                bytes = (String)value;
                buffer.put(bytes.getBytes(charset));
                buffer.put((byte)0);
                break;
            case 99:
                buffer.put(((Number)value).byteValue());
                break;
            case 102:
                buffer.putFloat(((Float)value).floatValue());
                break;
            case 105:
                buffer.putInt(((Integer)value).intValue());
                break;
            case 115:
                buffer.putShort(((Number)value).shortValue());
                break;
            default:
                throw new SAMFormatException("Unrecognized tag type: " + (char)tagType);
        }

        buffer.flip();
        byte[] bytes1 = new byte[buffer.limit()];
        buffer.get(bytes1);
        return bytes1;
    }

    private static void writeArray(Object value, boolean isUnsignedArray, ByteBuffer buffer) {
        int len$;
        int i$;
        if(value instanceof byte[]) {
            buffer.put((byte)(isUnsignedArray?67:99));
            byte[] array = (byte[])((byte[])value);
            buffer.putInt(array.length);
            byte[] arr$ = array;
            len$ = array.length;

            for(i$ = 0; i$ < len$; ++i$) {
                byte element = arr$[i$];
                buffer.put(element);
            }
        } else if(value instanceof short[]) {
            buffer.put((byte)(isUnsignedArray?83:115));
            short[] var8 = (short[])((short[])value);
            buffer.putInt(var8.length);
            short[] var11 = var8;
            len$ = var8.length;

            for(i$ = 0; i$ < len$; ++i$) {
                short var14 = var11[i$];
                buffer.putShort(var14);
            }
        } else if(value instanceof int[]) {
            buffer.put((byte)(isUnsignedArray?73:105));
            int[] var9 = (int[])((int[])value);
            buffer.putInt(var9.length);
            int[] var12 = var9;
            len$ = var9.length;

            for(i$ = 0; i$ < len$; ++i$) {
                int var15 = var12[i$];
                buffer.putInt(var15);
            }
        } else {
            if(!(value instanceof float[])) {
                throw new SAMException("Unrecognized array value type: " + value.getClass());
            }

            buffer.put((byte)102);
            float[] var10 = (float[])((float[])value);
            buffer.putInt(var10.length);
            float[] var13 = var10;
            len$ = var10.length;

            for(i$ = 0; i$ < len$; ++i$) {
                float var16 = var13[i$];
                buffer.putFloat(var16);
            }
        }

    }

    public static Object readSingleValue(byte tagType, ByteBuffer byteBuffer) {
        switch(tagType) {
            case 65:
                return Character.valueOf((char)byteBuffer.get());
            case 66:
                TagValueAndUnsignedArrayFlag valueAndFlag = readArray(byteBuffer);
                return valueAndFlag.value;
            case 67:
                return Integer.valueOf(byteBuffer.get() & 255);
            case 72:
                String hexRep = readNullTerminatedString(byteBuffer);
                return StringUtil.hexStringToBytes(hexRep);
            case 73:
                long val = (long)byteBuffer.getInt() & 4294967295L;
                if(val <= 2147483647L) {
                    return Integer.valueOf((int)val);
                }

                throw new RuntimeException("Tag value is too large to store as signed integer.");
            case 83:
                return Integer.valueOf(byteBuffer.getShort() & '\uffff');
            case 90:
                return readNullTerminatedString(byteBuffer);
            case 99:
                return Integer.valueOf(byteBuffer.get());
            case 102:
                return Float.valueOf(byteBuffer.getFloat());
            case 105:
                return Integer.valueOf(byteBuffer.getInt());
            case 115:
                return Integer.valueOf(byteBuffer.getShort());
            default:
                throw new SAMFormatException("Unrecognized tag type: " + (char)tagType);
        }
    }

    private static TagValueAndUnsignedArrayFlag readArray(ByteBuffer byteBuffer) {
        byte arrayType = byteBuffer.get();
        boolean isUnsigned = Character.isUpperCase(arrayType);
        int length = byteBuffer.getInt();
        Object value;
        int i;
        switch(Character.toLowerCase(arrayType)) {
            case 99:
                byte[] var9 = new byte[length];
                value = var9;
                byteBuffer.get(var9);
                return new TagValueAndUnsignedArrayFlag(value, isUnsigned);
            case 102:
                float[] var8 = new float[length];
                value = var8;

                for(i = 0; i < length; ++i) {
                    var8[i] = byteBuffer.getFloat();
                }

                return new TagValueAndUnsignedArrayFlag(value, isUnsigned);
            case 105:
                int[] var7 = new int[length];
                value = var7;

                for(i = 0; i < length; ++i) {
                    var7[i] = byteBuffer.getInt();
                }

                return new TagValueAndUnsignedArrayFlag(value, isUnsigned);
            case 115:
                short[] array = new short[length];
                value = array;

                for(i = 0; i < length; ++i) {
                    array[i] = byteBuffer.getShort();
                }

                return new TagValueAndUnsignedArrayFlag(value, isUnsigned);
            default:
                throw new SAMFormatException("Unrecognized tag array type: " + (char)arrayType);
        }
    }

    private static String readNullTerminatedString(ByteBuffer byteBuffer) {
        byteBuffer.mark();
        int startPosition = byteBuffer.position();

        while(byteBuffer.get() != 0) {
            ;
        }

        int endPosition = byteBuffer.position();
        byte[] buf = new byte[endPosition - startPosition - 1];
        byteBuffer.reset();
        byteBuffer.get(buf);
        byteBuffer.get();
        return StringUtil.bytesToString(buf);
    }
}

