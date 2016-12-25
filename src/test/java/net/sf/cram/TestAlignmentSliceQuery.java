package net.sf.cram;


import htsjdk.samtools.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestAlignmentSliceQuery {

    @Test
	public void testPositive() {
        // test positive cases
        doTestPositive("abcd", "", null, "", null);
        doTestPositive("ABCD", "", null, "", null);

        doTestPositive("ABCD", ":", 100, "", null);
        doTestPositive("ABCD", ":", 100, "-", 500);
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
    public void testNegative1() {
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery(null);
    }

    @Test(expected=ArrayIndexOutOfBoundsException.class)
    public void testNegative2() {
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery(":-");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative3() {
        // test negative case
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:-1");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative4() {
        // test negative case
        // start underflow
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:-1-100");
    }

    @Test(expected=NumberFormatException.class)
    public void testNegative5() {
        // test negative case
        // end overflow
        AlignmentSliceQuery asq = new AlignmentSliceQuery("ABCD:1-2147483648");
    }

    private void doTestPositive(String sequence, String sep1, Integer start, String sep2, Integer end)
    {
        Assert.assertNotNull(sequence);
        Assert.assertNotNull(sep1);
        Assert.assertNotNull(sep2);

        Assert.assertFalse(
                "Broken Test - 1sr parameter should not contain ':'",
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
