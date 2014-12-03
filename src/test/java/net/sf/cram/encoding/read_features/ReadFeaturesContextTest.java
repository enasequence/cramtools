package net.sf.cram.encoding.read_features;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import net.sf.cram.encoding.read_features.ReadFeaturesContext;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarOperator;

import org.junit.Before;
import org.junit.Test;

public class ReadFeaturesContextTest {

	private ReadFeaturesContext readFeaturesContext;
	private byte[] bases;
	private byte[] refs;

	@Before
	public void before() {
		refs = new byte[1000];
		Arrays.fill(refs, (byte) 'A');
		bases = new byte[20];
		readFeaturesContext = new ReadFeaturesContext(1, refs, bases, null);
	}

	@Test
	public void testEmpty() {
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("20M"));
		assertThat(bases, is("AAAAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testMismatchAt_1() {
		readFeaturesContext.addCigarElementAt(1, CigarOperator.M, 1);
		bases[0] = (byte) 'C';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("20M"));
		assertThat(bases, is("CAAAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testMismatchAt_2() {
		readFeaturesContext.addCigarElementAt(2, CigarOperator.M, 1);
		bases[1] = (byte) 'C';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("20M"));
		assertThat(bases, is("ACAAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testLongMismatchAt() {
		readFeaturesContext.addCigarElementAt(1, CigarOperator.M, 2);
		bases[0] = (byte) 'C';
		bases[1] = (byte) 'C';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("20M"));
		assertThat(bases, is("CCAAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testInsert_1() {
		readFeaturesContext.addCigarElementAt(1, CigarOperator.I, 1);
		bases[0] = (byte) 'C';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("1I19M"));
		assertThat(bases, is("CAAAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testLongInsert() {
		readFeaturesContext.addCigarElementAt(2, CigarOperator.I, 2);
		bases[1] = (byte) 'C';
		bases[2] = (byte) 'C';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("1M2I17M"));
		assertThat(bases, is("ACCAAAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testInsertAndMismatch() {
		readFeaturesContext.addCigarElementAt(2, CigarOperator.I, 2);
		readFeaturesContext.addCigarElementAt(5, CigarOperator.M, 2);
		bases[1] = (byte) 'C';
		bases[2] = (byte) 'C';

		bases[4] = (byte) 'G';
		bases[5] = (byte) 'G';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("1M2I17M"));
		assertThat(bases, is("ACCAGGAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testInsertAndDeleteAndMismatch() {
		System.arraycopy("ACGTN".getBytes(), 0, refs, 0, 5);

		readFeaturesContext.addCigarElementAt(2, CigarOperator.I, 2);
		readFeaturesContext.addCigarElementAt(4, CigarOperator.D, 1);
		readFeaturesContext.addCigarElementAt(5, CigarOperator.M, 2);
		bases[1] = (byte) 'c';
		bases[2] = (byte) 'c';

		bases[4] = (byte) 'g';
		bases[5] = (byte) 'g';
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("1M2I1D17M"));
		assertThat(new String(bases), bases,
				is("AccGggAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void testMismatchAndMismatch() {
		System.arraycopy("ACGTN".getBytes(), 0, refs, 0, 5);

		readFeaturesContext.addMismatch(2, (byte) 'c');
		readFeaturesContext.addMismatch(4, (byte) 't');
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("20M"));
		assertThat(new String(bases), bases,
				is("AcGtNAAAAAAAAAAAAAAA".getBytes()));
	}

	@Test
	public void test_100_82M1D3M1I8M1I5M() {
		refs = "CTCTTGTGTGCACACAGCACAGCCTCTACTGCTACACCTGAGTACTTTGCCAGTGGCCTGGAAGCACTTTGTCCCCCCTGGCACAAATGGTGCTGGACCACGAGGGGCCAGAGAACAAAGCCTTGGGCGTGGTCCCAACTCCCAAATGTTTGAACACACAAGTTGGAATATTGGGCTGAGATCTGTGGCCAGGGCCTGAGT"
				.getBytes();
		bases = new byte[100];
		readFeaturesContext = new ReadFeaturesContext(1, refs, bases, null);

		readFeaturesContext.addMismatch(64, (byte) 'C');
		readFeaturesContext.addMismatch(68, (byte) 'A');
		readFeaturesContext.addMismatch(79, (byte) 'C');
		readFeaturesContext.addMismatch(80, (byte) 'C');
		readFeaturesContext.addCigarElementAt(83, CigarOperator.D, 1);
		readFeaturesContext.addCigarElementAt(86, CigarOperator.I, 1);
		readFeaturesContext.injectBase(86, (byte) 67);
		readFeaturesContext.addMismatch(88, (byte) 'A');
		readFeaturesContext.addMismatch(91, (byte) 'C');
		readFeaturesContext.addCigarElementAt(95, CigarOperator.I, 1);
		readFeaturesContext.injectBase(95, (byte) 65);
		readFeaturesContext.addMismatch(98, (byte) 'C');
		readFeaturesContext.finish();
		Cigar cigar = readFeaturesContext.createCigar();

		assertThat(cigar.toString(), is("82M1D3M1I8M1I5M"));
		assertThat(
				new String(bases),
				bases,
				is("CTCTTGTGTGCACACAGCACAGCCTCTACTGCTACACCTGAGTACTTTGCCAGTGGCCTGGAACCACATTGTCCCCCCCCGCCAACAAGGCGCTAGGCCC"
						.getBytes()));
	}
}
