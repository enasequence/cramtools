/***************************************************
(Test 2)
Junit Test Cases to test following.

Test static inner class: 
1) Version

Test the following methods:
1) reverse(final byte[] array, int offset, int len)
2) reverseComplement(final byte[] bases, int offset, int len)
3) complement(final byte b)
4) upperCase(byte base)
5) byte[] upperCase(byte[] bases)
6) String calculateMD5String(byte[] data)
7) String calculateMD5String(byte[] data, int offset, int len) 
8) byte[] calculateMD5(byte[] data, int offset, int len)

Submitted by: SHIVIKA SHARMA 
****************************************************/
package net.sf.cram.common;

import org.junit.Test;
import org.junit.Ignore;
//import org.junit.Rule;
//import org.junit.rules.ExpectedException;
import static org.junit.Assert.*;
import net.sf.cram.common.Utils;
import net.sf.cram.common.Utils.Version;
import java.util.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UtilsTest {



	@Test
	public void testVersion1(){
		
		Version ver = new Version(3,1,0);
		assertEquals("Major version value is different than expected value", 3, ver.major);
		assertEquals("Minor version value is different than expected value", 1, ver.minor);
		assertEquals("Build version value is different than expected value", 0, ver.build);
		assertEquals("Different than expected Version toString() output", "3.1", ver.toString());
	}


	@Test
	public void testVersion2(){
		
		Version ver = new Version(3,1,1);
		assertEquals("Build version value is different than expected value", 1, ver.build);
		assertTrue("Version(int, int, int) toString() output is wrong", "3.1-b1".equals(ver.toString()));
	}


	@Test
	public void testVersion_withNegativeValue(){
		
		Version ver = new Version(-4,2,1);
		fail("Version can not be negative");
	}

	
	@Test
	public void testVersion4(){
		
		Version ver = new Version("4.1-b23");
		assertEquals("Major version value is different than expected value", 4, ver.major);
		assertEquals("Minor version value is different than expected value", 1, ver.minor);
		assertEquals("Build version value is different than expected value",23, ver.build);
		assertTrue("Different than expected Version toString() output", "4.1-b23".equals(ver.toString()));
	}

	
	@Test
	public void testVersion5(){
		
		Version ver = new Version("3.0-b0");
		assertEquals("Major version value is different than expected value", 3, ver.major);
		assertEquals("Minor version value is different than expected value", 0, ver.minor);
		assertEquals("Build version value is different than expected value", 0, ver.build);
		assertTrue("Different than expected Version toString() output", "3.0".equals(ver.toString()));
	}


	@Test
	public void testVersion_wrongVersionFormat(){
		
		Version ver = new Version("3.0.6");
		assertEquals("Major version value is different than expected value", 3, ver.major);
		assertEquals("Minor version value is different than expected value", 0, ver.minor);
		assertEquals("Build version specified in wrong format.", 6, ver.build);
	}

	@Test(expected=NumberFormatException.class)
	public void testVersion7_throwsException(){
		
		Version ver = new Version("3.0.6-b2");
		
	}

	@Test(expected=NumberFormatException.class)
	public void testVersion8_withNullString(){
		
		Version ver = new Version("");
	}
	
		
	@Test
	public void testReverse_ByteArray(){
		
		final byte[] arr = "acbtng".getBytes();
		
		final byte[] expectedArr = "gntbca".getBytes();
		
		Utils.reverse(arr, 0, 6);
		//assertTrue("Actual reversed array is different from Expected Array",Arrays.equals(expectedArr, arr));
		assertArrayEquals("Actual reversed array is different from Expected Array", expectedArr, arr);
	}

	
	@Test
	public void testReverse_OffsetNonZero(){
		
		final byte[] arr = "actgnAT".getBytes();
		
		final byte[] expectedArr = "acTAngt".getBytes();
		
		Utils.reverse(arr, 2, 5);
		assertArrayEquals("Actual reversed array is different from Expected Array", expectedArr, arr);
	}
	
	
	@Test
	public void testReverse_ByteArrayError(){

		final byte[] arr = "actgn".getBytes();
		
		final byte[] expectedArr = "atgcn".getBytes();
		
		Utils.reverse(arr, 1,3);
		assertFalse("Reverse array is supposed to be different from expected array.",Arrays.equals(expectedArr, arr));
	}

	
	@Test
	public void testReverse_WithZeroLength(){

		final byte[] arr = "gtnca".getBytes();
		
		final byte[] expectedArr = "gtnca".getBytes();
		Utils.reverse(arr, 0, 0);
		assertArrayEquals("Actual reversed array is different from Expected Array", expectedArr, arr);
	}

	
	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testReverse_WhenArrayShorterThenLen(){
		
		final byte[] arr = "actgn".getBytes();
		final byte[] expectedArr = "ngtca".getBytes();
		
		Utils.reverse(arr, 0, 6);
	}

	
	@Test
	public void testReverse_WithNegLength(){

		final byte[] arr = "actgn".getBytes();
		final byte[] expectedArr = "actgn".getBytes();
		
		Utils.reverse(arr, 1, -3);   
		// Array arr will remain same
		assertArrayEquals("Actual reversed array is different from Expected Array", expectedArr, arr);
	}

	
	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testReverse_WithNegOffset(){

		final byte[] arr = "actgn".getBytes();
		final byte[] expectedArr = "agtcn".getBytes();
		
		Utils.reverse(arr, -1, 3);   
	}

	
	@Test
	public void testReverseComplement1(){

		final byte[] arr = "actgn".getBytes();
		final byte[] expectedArr = "ncagt".getBytes();
		
		Utils.reverseComplement(arr, 0, 5);
		assertArrayEquals("Actual reverse complement array is different from Expected Array", expectedArr, arr);
	}

	
	@Test
	public void testReverseComplement_withNonZeroOffset(){

		final byte[] arr = "actaGtNaATGgan".getBytes();
		final byte[] expectedArr = "actcCATtNaCtan".getBytes();
		
		Utils.reverseComplement(arr, 3, 9);
		assertArrayEquals("Actual reverse complement array is different from Expected Array", expectedArr, arr);
	}

	
	@Test
	public void testReverseComplement_withZeroLength(){

		final byte[] arr = "actag".getBytes();
		final byte[] expectedArr = "actag".getBytes();
		
		Utils.reverseComplement(arr, 3, 0);
		//Array arr will remain same.
		assertArrayEquals("Actual reverse complement array is different from Expected Array", expectedArr, arr);
	}

	
	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testReverseComplement_withNegOffset(){

		final byte[] arr = "actag".getBytes();
		final byte[] expectedArr = "actag".getBytes();
		
		Utils.reverseComplement(arr, -2, 3);
	}

	
	@Test(expected=NullPointerException.class)
	public void testReverseComplement_withNullArray(){

		final byte[] arr = null;
				
		Utils.reverseComplement(arr, 0, 3);
	}


	
	@Test
	public void testComplement(){
		
		assertTrue("Complement base value of 'a' must be 't'", Utils.complement((byte) 'a') ==  (byte) 't');
		assertTrue("Complement base value of 'c' must be 'g'", Utils.complement((byte) 'c') ==  (byte) 'g');
		assertTrue("Complement base value of 'g' must be 'c'", Utils.complement((byte) 'g') ==  (byte) 'c');
		assertTrue("Complement base value of 't' must be 'a'", Utils.complement((byte) 't') ==  (byte) 'a');
		assertTrue("Complement base value of 'A' must be 'T'", Utils.complement((byte) 'A') ==  (byte) 'T');
		assertTrue("Complement base value of 'C' must be 'G'", Utils.complement((byte) 'C') ==  (byte) 'G');
		assertTrue("Complement base value of 'G' must be 'C'", Utils.complement((byte) 'G') ==  (byte) 'C');
		assertTrue("Complement base value of 'T' must be 'A'", Utils.complement((byte) 'T') ==  (byte) 'A');
		assertTrue("Complement value of 'n' will be 'n'", Utils.complement((byte) 'n') ==  (byte) 'n');
		assertTrue("Complement value of 'N' will be 'N'", Utils.complement((byte) 'N') ==  (byte) 'N');
		
	}

	
	@Test
	public void testUpperCase(){
		
		assertTrue("Must return base value 'a' in Upper Case", Utils.upperCase((byte) 'a') ==  (byte) 'A');
		assertTrue("Must return base value 'c' in Upper Case", Utils.upperCase((byte) 'c') ==  (byte) 'C');
		assertTrue("Must return base value 'g' in Upper Case", Utils.upperCase((byte) 'g') ==  (byte) 'G');
		assertTrue("Must return base value 't' in Upper Case", Utils.upperCase((byte) 't') ==  (byte) 'T');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'A') ==  (byte) 'A');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'C') ==  (byte) 'C');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'G') ==  (byte) 'G');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'T') ==  (byte) 'T');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'n') ==  (byte) 'N');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'N') ==  (byte) 'N');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 'u') ==  (byte) 'U');
		assertTrue("Must return character in upper case else return as it is", Utils.upperCase((byte) 23) ==  (byte) 23);
		
	}

	
	@Test
	public void testUpperCaseByteArray_1(){
		
		byte[] arr = "actgre".getBytes();
		final byte[] result = Utils.upperCase(arr);
		final byte[] expectedArr = "ACTGRE".getBytes();
		assertArrayEquals("Resulted array is different than expected array", expectedArr, result);
	}

	
	@Test
	public void testUpperCaseByteArray_2(){
		
		byte[] arr = "act7*#AB".getBytes();
		final byte[] result = Utils.upperCase(arr);
		final byte[] expectedArr = "ACT7*#AB".getBytes();
		assertArrayEquals("Resulted array is different than expected array", expectedArr, result);
		
	}

	
	@Test(expected=NullPointerException.class)
	public void testUpperCaseByteArray_3(){
		
		byte[] arr = null;
		final byte[] result = Utils.upperCase(arr);
	}

	
	@Test
	public void testCalculateMD5String_1()throws NoSuchAlgorithmException{

		byte[] arr = "actgn".getBytes();
		String expectedResult = MD5HashForTest(arr);
		assertEquals("Expected MD5 hash is different than actual hash", expectedResult, Utils.calculateMD5String(arr));
	}

	
	@Test
	public void testCalculateMD5String_2() throws NoSuchAlgorithmException{
		byte[] arr = "".getBytes();
		String expectedResult = MD5HashForTest(arr);
		assertEquals("Expected MD5 hash is different than actual hash", expectedResult, Utils.calculateMD5String(arr));
	}

	
	@Test
	public void testCalculateMD5String_WithOffset() throws NoSuchAlgorithmException{
		byte[] arr = "actgactggnact".getBytes();
		byte[] testArr = "gactgg".getBytes();
		String expectedResult = MD5HashForTest(testArr);
		assertEquals("Expected MD5 hash is different than actual hash", expectedResult, Utils.calculateMD5String(arr, 3, 6));
	}

	/**
	* A MD5 hash converted to hex should always be 32 characters.
	*/
	@Test
	public void testCalculateMD5String_HexLength(){
		byte[] arr = "actgactggnact".getBytes();
		String hash = Utils.calculateMD5String(arr, 0, arr.length);
		assertEquals("Expected MD5 hash hex length is 32 characters", 32, hash.length());

		arr = "somearraygreaterthanthirtytwolengthsize".getBytes();
		hash = Utils.calculateMD5String(arr, 0, arr.length);
		assertEquals("Expected MD5 hash hex length is 32 characters", 32, hash.length());
	}


	@Test
	public void testCalculateMD5_1()throws NoSuchAlgorithmException{
		byte[] arr = "actgtacat".getBytes();
		byte[] testArray1 = "gtac".getBytes();

		MessageDigest md5 = MessageDigest.getInstance("MD5");

		assertTrue("Expected Digest is different than actual Digest", md5.isEqual(md5.digest(arr), Utils.calculateMD5(arr, 0, arr.length)));
		assertTrue("Expected Digest is different than actual Digest", md5.isEqual(md5.digest(testArray1), Utils.calculateMD5(arr, 3, 4)));
	}

	/**
	* A MD5 hash should always be a 16 element byte[].
	*/
	@Test
	public void testCalculateMD5_2(){
		byte[] arr = "actgtacat".getBytes();
		byte[] digest = Utils.calculateMD5(arr, 0, arr.length);

		assertEquals("MD5 hash is should always be a 16 element byte[].", 16, digest.length);
	}


	private static String MD5HashForTest(byte[] array) throws NoSuchAlgorithmException{
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		byte[] digest = md5.digest(array);
		return String.format("%032x", new BigInteger(1, digest));
	} 


}

