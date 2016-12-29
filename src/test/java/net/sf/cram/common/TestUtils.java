package net.sf.cram.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static net.sf.cram.common.Utils.complement;

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
        //simple test 1
        byte[] array = new byte[] {1,2,3,4,5};
        Utils.reverse(array, 0, 5);
        Assert.assertArrayEquals(new byte[] {5,4,3,2,1}, array);

        //simple test 2
        byte[] array2 = new byte[] {1,2,3,4,5};
        Utils.reverse(array2, 2, 2);
        Assert.assertArrayEquals(new byte[] {1,2,4,3,5}, array2);

        // randomized test
        Random rnd = new Random();
        byte[] original = new byte[rnd.nextInt(1000)];
        rnd.nextBytes(original);
        byte[] copy = original.clone();
        Utils.reverse(copy, rnd.nextInt(copy.length), 1);
        Assert.assertArrayEquals(original, copy);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void TestReverseNegative1()
            throws ArrayIndexOutOfBoundsException{
        //index out of bounds text
        byte[] array = new byte[]{1, 2, 3, 4, 5};
        Utils.reverse(array, 100, 1);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void TestReverseNegative2()
            throws ArrayIndexOutOfBoundsException{
        //index out of bounds text
        byte[] array = new byte[]{1, 2, 3, 4, 5};
        Utils.reverse(array, 1, 100);
    }

    @Test
    public void TestComplement() {

        final byte a = 'a', c = 'c', g = 'g', t = 't', n = 'n', A = 'A', C = 'C', G = 'G', T = 'T', N = 'N';

        Map<Byte,Byte> map = new HashMap<>();
        map.put( a ,  t );
        map.put( c ,  g );
        map.put( g ,  c );
        map.put( t ,  a );
        map.put( A ,  T );
        map.put( C ,  G );
        map.put( G ,  C );
        map.put( T ,  A );

        // test acgtACGT values
        map.forEach( (k,v) -> Assert.assertTrue( "Base compliment broken", complement(k) == v) );

        // test other values
        Random rnd = new Random();
        byte[] array = new byte[100];
        rnd.nextBytes(array);

        for (byte b : array) {
            if (!map.containsKey(b))
                Assert.assertTrue("Base compliment broken", complement(b) == b);
        }
    }









}
