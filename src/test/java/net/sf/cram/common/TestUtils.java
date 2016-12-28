package net.sf.cram.common;

import org.junit.Assert;
import org.junit.Test;

public class TestUtils {

    @Test
    public void testVersionPositive() {
        // test positive cases
        doTestVersionInt(1,2,3);
        doTestVersionStr(1,2,3);

        // test boundary conditions
        doTestVersionInt(0,0,0);
        doTestVersionStr(0,0,0);
        doTestVersionInt(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
        doTestVersionStr(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
    }

    @Test
    public void testVersionNegative()
    {
        doTestVersionNegative("Major version should not be negative",-1, 2, 3 );
        doTestVersionNegative("Major version should not be negative","-1.2-b3");

        doTestVersionNegative("Minor version should not be negative",1, -2, 3 );
        doTestVersionNegative("Minor version should not be negative","1.-2-b3");

        doTestVersionNegative("Build version should not be negative",1, 2, -3 );
        doTestVersionNegative("Build version should not be negative","1.2-b-3");
    }

    private void doTestVersionNegative(String message, Object... args)
    {

        Assert.assertTrue(args!=null && args.length>0);

        try {
            // test negative case
            // illegal input
            if(args[0] instanceof String) {
                Utils.Version v = new Utils.Version((String)(args[0]));
            }else
            if(args[0] instanceof Integer && args.length==3) {
                Utils.Version v = new Utils.Version((int)(args[0]),(int)(args[1]),(int)(args[2]));
            }else
                Assert.fail("Broken test");

        } catch (IllegalArgumentException e) {
            return;
        }catch(Exception all){}

        Assert.fail(message);
    }



    private void doTestVersionInt(int major, int minor, int build)
    {
        String testStr =
                (build > 0) ?
                String.format("%d.%d-b%d", major, minor, build):
                String.format("%d.%d", major, minor);

        Utils.Version v = new Utils.Version(major, minor, build);
        Assert.assertEquals(major, v.major);
        Assert.assertEquals(minor, v.minor);
        Assert.assertEquals(build, v.build);

        Assert.assertEquals(testStr, v.toString());
    }

    private void doTestVersionStr(int major, int minor, int build)
    {
        String testStr =
                (build > 0) ?
                String.format("%d.%d-b%d", major, minor, build):
                String.format("%d.%d", major, minor);

        Utils.Version v = new Utils.Version(testStr);
        Assert.assertEquals(major, v.major);
        Assert.assertEquals(minor, v.minor);
        Assert.assertEquals(build, v.build);

        Assert.assertEquals(testStr, v.toString());
    }

    @Test
    public void TestReverse()
    {
        byte[] array = new byte[] {1,2,3,4,5};
        Utils.reverse(array, 0, 0);
        Assert.assertArrayEquals(new byte[] {1,2,3,4,5}, array);
    }








}
