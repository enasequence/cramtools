/***************************************************
(Test 1)
Junit Test Cases to test Class AlignmentSliceQuery.java.
The class is designed to parse and print genome location queries of form:
sequenceName[:start[-end]]
where
sequenceName:    string matching pattern [A-Za-z0-1_-|]+
start:    an integer [1, 2^31]
end:    an integer [1, 2^31]

Submitted by: SHIVIKA SHARMA 
****************************************************/

package net.sf.cram;

import net.sf.cram.AlignmentSliceQuery;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;


public class AlignmentSliceQueryTest {

	@Test
	public void testParseSequenceName(){
		
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1");
		
		assertEquals("Parsed Sequence name is different than expected.", "chr1", asq.sequence);
		assertEquals("Sequence Start is different than expected default value.",0,asq.start);
		assertEquals("Sequence End is different than expected default value.",Integer.MAX_VALUE,asq.end);
	}
	

	@Test
	public void testParseLocationString1(){
		 
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1:10000");
		
		assertEquals("Parsed Sequence name is different than expected.", "chr1", asq.sequence);
		assertEquals("Parsed Sequence Start is different than expected.",10000,asq.start);
		assertEquals("Sequence End is different than expected default value.",Integer.MAX_VALUE,asq.end);
	}

	
	@Test
	public void testParseLocationString2(){
		/*
		Assuming START and END take integer values between [1-Integer.MAX_VALUE] Inclusive.
		*/
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1:1");   

		assertEquals("Parsed Sequence name is different than expected.", "chr1", asq.sequence);
		assertEquals("Parsed Sequence Start is different than expected.",1,asq.start);
	}

	
	@Test
	public void testParseLocationString3(){
		
		AlignmentSliceQuery asq = new AlignmentSliceQuery("ENA|U00096|U00096.3:3000000-3001000");
	
		assertEquals("Parsed Sequence name is different than expected.", "ENA|U00096|U00096.3", asq.sequence);
		assertEquals("Parsed Sequence Start is different than expected.",3000000,asq.start);
		assertEquals("Parsed Sequence End is different than expected.",3001000,asq.end);		  
	
	}


	@Test(expected=NumberFormatException.class)
	public void testLocationStringWithInvalidInput1(){
		
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1:10a");
	}


	@Test(expected=NumberFormatException.class)
	public void testLocationStringWithInvalidInput2(){
		/*
		When END value is greater than Integer.MAX_VALUE
		*/
		AlignmentSliceQuery asq = new AlignmentSliceQuery("ENA|U00096|U00096.3:3000000-2147483648");
	}

	
	@Test
	public void testSequenceNameMatchPattern1(){
		/*
		Considering Sequence Name must be a string of Regex matching value [A-Za-z0-1_-|]+  (As per provided in the MAIL)
		*/ 
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1:3000000-3001000");
		
		String expectedSeq = asq.sequence;
		assertTrue(expectedSeq.matches("[A-Za-z0-1_-|]+"));
	}

	
	@Test
	public void testSequenceNameMatchPattern2(){
		/*
		Considering Sequence Name must be a string of Regex matching value [A-Za-z0-1_-|]+  (As per provided in the MAIL)
		*/
		AlignmentSliceQuery asq = new AlignmentSliceQuery("ENA|U00096|U00096.3:3000000-3001000");
		
		String expectedSeq = asq.sequence;
		assertTrue("Actual Sequence Name does not matches with expected Regular Expression.", expectedSeq.matches("[A-Za-z0-1_-|]+")); 
	}

	
	@Test
	public void testToString1(){
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr10:1000-30000");
		
		assertTrue("Expected toString() output is different from Actual String.", "chr10:1000-30000".equals(asq.toString()));			
	}
	

	@Test
	public void testToString2(){
		/*
		Assuming variable Start can take values between [1 - Integer.MAX_VALUE] inclusive.
		*/
		AlignmentSliceQuery asq = new AlignmentSliceQuery("chr1:1");
		
		assertTrue("Expected toString() output is different from Actual String.", "chr1:1".equals(asq.toString()));			
	}

		
	@Test
	public void testEmptyString(){

		AlignmentSliceQuery asq = new AlignmentSliceQuery("");
		
		assertEquals("",asq.sequence);
		fail("The Alignment String is empyt");

	}

}