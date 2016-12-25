package net.sf.cram;


import com.sun.org.glassfish.gmbal.Description;
import htsjdk.samtools.util.StringUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


/*
AlignmentSliceQuery class is designed to parse and print genome location queries of form:
sequenceName[:start[-end]]

where
sequenceName:    string matching pattern [A-Za-z0-1_-|]+
start:    an integer [1, 2^31]
end:    an integer [1, 2^31]

examples:
chr1
X:10000
ENA|U00096|U00096.3:3000000-3001000
*/

public class TestAlignmentSliceQuery {

    @Test
    public void testPositive() {
        // test positive cases
        doTestPositive("abcd", "", null, "", null);
        doTestPositive("ABCD", "", null, "", null);

        doTestPositive("ABCD", ":", 100, "", null);
        doTestPositive("ABCD", ":", 100, "-", 500);

        doTestPositive("chr1", "", null, "", null);
        doTestPositive("X", ":", 10000, "", null);

        doTestPositive("ENA|U00096|U00096.3", ":", 3000000, "-", 3001000);
    }

    @Test
    public void testBoundary() {
        // test boundary conditions
        doTestPositive("", "", null, "", null);
        doTestPositive("ABCD", ":", 0, "", null);
        doTestPositive("ABCD", ":", 1, "", null);

        doTestPositive("ABCD", ":", 0, "-", 2147483647);
        doTestPositive("ABCD", ":", 0, "-", Integer.MAX_VALUE);
    }

    @Test(expected=NullPointerException.class)
    public void testNegative1()  throws NullPointerException{
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery(null);
    }

    @Test(expected=ArrayIndexOutOfBoundsException.class)
    public void testNegative2()  throws  ArrayIndexOutOfBoundsException{
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery(":-");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative3()  throws NumberFormatException {
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:-1");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative4()  throws NumberFormatException{
        // test negative case
        // start underflow
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:-1-100");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative5() throws NumberFormatException {
        // test negative case
        // end overflow
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:1-2147483648");
    }

    @Ignore("Broken test - not sure how tested class should handle 0 as start index")
    @Test(expected=IndexOutOfBoundsException.class)
    public void testNegative6()  throws IndexOutOfBoundsException {
        // test negative case
        // start underflow ( not supposed to start with 0 )
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:0-100");
    }

    @Test
    public void testNegative7() {
        try {
            // test negative case
            // illegal characters
            AlignmentSliceQuery asq = new AlignmentSliceQuery("!@#$%");

        } catch (IllegalArgumentException e) {
            return;
        }catch(Exception all){}

        Assert.fail("Alignment should not contain illegal characters outside of [A-Za-z0-1_-|]+ range");
    }


    private void doTestPositive(String sequence, String sep1, Integer start, String sep2, Integer end)
    {
        Assert.assertNotNull(sequence);
        Assert.assertNotNull(sep1);
        Assert.assertNotNull(sep2);

        Assert.assertFalse(
                "Broken Test - 1st parameter should not contain ':'",
                sequence.contains(":"));

        String testStr =
                sequence
                + sep1 + (start == null?"":start.toString())
                + sep2 + (end == null  ?"":end.toString()  );

        AlignmentSliceQuery asq = new AlignmentSliceQuery(testStr);

        Assert.assertEquals( asq.sequence, sequence );

        Assert.assertEquals( asq.start,  start == null ? 0 : start       );
        Assert.assertEquals( asq.end,    end == null ? 2147483647 : end  );

        if(start != null && end != null)
        {
            if (start > 1 || end < Integer.MAX_VALUE)
                Assert.assertEquals(testStr, asq.toString());
        }
    }

}
