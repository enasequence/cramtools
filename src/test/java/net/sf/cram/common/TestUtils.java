package net.sf.cram.common;

import org.junit.Assert;
import org.junit.Test;


import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

import static net.sf.cram.common.Utils.complement;
import static net.sf.cram.common.Utils.upperCase;

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
        // empty array
        byte[] arrayEmpty = {};
        Utils.reverse(arrayEmpty, 0, 0);
        Assert.assertArrayEquals(new byte[]{}, arrayEmpty);

        // null array
        byte[] arrayNull = null;
        Utils.reverse(arrayNull, 0, 0);
        Assert.assertArrayEquals(null, arrayNull);

        //one element array
        byte[] arrayOne = new byte[] {1};
        Utils.reverse(arrayOne, 0, 1);
        Assert.assertArrayEquals(new byte[] {1}, arrayOne);

        //simple test 1
        byte[] array = new byte[] {1,2,3,4,5};
        Utils.reverse(array, 0, 5);
        Assert.assertArrayEquals(new byte[] {5,4,3,2,1}, array);

        //simple test 2
        byte[] array2 = new byte[] {1,2,3,4,5};
        Utils.reverse(array2, 2, 2);
        Assert.assertArrayEquals(new byte[] {1,2,4,3,5}, array2);

        // randomized test
        doTestReverseRandom(false);
    }

    @Test
    public void TestReverseComplement()
    {
        // empty array
        byte[] arrayEmpty = {};
        Utils.reverseComplement(arrayEmpty, 0, 0);
        Assert.assertArrayEquals(new byte[]{}, arrayEmpty);

        // null array
        byte[] arrayNull = null;
        Utils.reverseComplement(arrayNull, 0, 0);
        Assert.assertArrayEquals(null, arrayNull);

        //one element array
        byte[] arrayOne = new byte[] {'a'};
        Utils.reverseComplement(arrayOne, 0, 1);
        Assert.assertArrayEquals(new byte[] {'t'}, arrayOne);

        //simple test 1
        byte[] array = new byte[] {'a','c','g','t'};
        Utils.reverseComplement(array, 0, 4);
        Assert.assertArrayEquals(new byte[] {'a','c','g','t'}, array);

        //simple test 2
        byte[] array2 = new byte[] {'A','C','G','T'};
        Utils.reverseComplement(array2, 2, 2);
        Assert.assertArrayEquals(new byte[] {'A','C','A','C'}, array2);

        // randomized test
        doTestReverseRandom(true);

    }
    private void doTestReverseRandom(boolean bComplement)
    {
        // randomized test
        Random rnd = new Random();
        byte[] original = new byte[1+rnd.nextInt(1000)];
        rnd.nextBytes(original);
        byte[] copy = original.clone();

        int offset = rnd.nextInt(copy.length);
        int len = rnd.nextInt(copy.length-offset);

        if(bComplement)
            Utils.reverseComplement(copy, offset, len);
        else
            Utils.reverse(copy, offset, len);

        // test that the head is unchanged
        Assert.assertArrayEquals(
                Arrays.copyOfRange(original,0,offset),
                Arrays.copyOfRange(copy,0,offset)
        );

        Function<Byte,Byte> wrapper =
                bComplement ? Utils::complement : (b)->b;

        //test the reversed part
        byte[] sliceOriginal = Arrays.copyOfRange(original, offset, offset + len);
        byte[] sliceCopy = Arrays.copyOfRange(copy, offset, offset + len);

        Assert.assertArrayEquals(
                IntStream.range(1, sliceOriginal.length + 1).boxed()
                        .mapToInt(i -> wrapper.apply(sliceOriginal[sliceOriginal.length - i])).toArray()
                ,
                IntStream.range(0, sliceCopy.length).boxed()
                        .mapToInt(i -> sliceCopy[i]).toArray()
        );

        // test that the tail is unchanged
        Assert.assertArrayEquals(
                Arrays.copyOfRange(original,offset+len,original.length),
                Arrays.copyOfRange(copy,offset+len,copy.length)
        );
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


    @Test
    public void TestUpperCase()
    {
        final byte a = 'a', c = 'c', g = 'g', t = 't', n = 'n', A = 'A', C = 'C', G = 'G', T = 'T', N = 'N';

        Map<Byte,Byte> map = new HashMap<>();
        map.put( a ,  A );
        map.put( c ,  C );
        map.put( g ,  G );
        map.put( t ,  T );
        map.put( n ,  N );

        map.forEach( (k,v) -> Assert.assertTrue( "upperCase broken", upperCase(k) == v) );

        byte[] base = new byte[]{a,c,g,t,n};
        byte[] baseUpper = new byte[]{A,C,G,T,N};

        Assert.assertArrayEquals(baseUpper, upperCase(base));
    }

    @Test
    public void TestCalculateMD5() {

        Assert.assertEquals(
                "d41d8cd98f00b204e9800998ecf8427e",
                Utils.calculateMD5String(new byte[]{})
        );
        Assert.assertEquals(
                "0cc175b9c0f1b6a831c399e269772661",
                Utils.calculateMD5String(new byte[]{'a'})
        );

        Assert.assertEquals(
                "900150983cd24fb0d6963f7d28e17f72",
                Utils.calculateMD5String("abc".getBytes())
        );

        Assert.assertEquals(
                "f96b697d7cb7938d525a2f31aaf161d0",
                Utils.calculateMD5String("message digest".getBytes())
        );

        Assert.assertEquals(
                "c3fcd3d76192e4007dfb496cca67e13b",
                Utils.calculateMD5String("abcdefghijklmnopqrstuvwxyz".getBytes())
        );

        Assert.assertEquals(
                "d174ab98d277d9f5a5611c2c9f419d9f",
                Utils.calculateMD5String("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes())
        );

        Assert.assertEquals(
                "57edf4a22be3c955ac49da2e2107b67a",
                Utils.calculateMD5String("12345678901234567890123456789012345678901234567890123456789012345678901234567890".getBytes())
        );

        Assert.assertEquals(
                "d41d8cd98f00b204e9800998ecf8427e",
                Utils.calculateMD5String(new byte[]{'a','b'},1,0)
        );
        Assert.assertEquals(
                "0cc175b9c0f1b6a831c399e269772661",
                Utils.calculateMD5String(new byte[]{'b','a','b'},1,1)
        );

        Assert.assertEquals(
                "900150983cd24fb0d6963f7d28e17f72",
                Utils.calculateMD5String("xabcx".getBytes(),1,3)
        );

        Assert.assertEquals(
                "900150983cd24fb0d6963f7d28e17f72",
                Utils.calculateMD5String("abcx".getBytes(),0,3)
        );

        Assert.assertEquals(
                "900150983cd24fb0d6963f7d28e17f72",
                Utils.calculateMD5String("xabc".getBytes(),1,3)
        );
    }


}
