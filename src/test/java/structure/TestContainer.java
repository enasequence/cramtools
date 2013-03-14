package structure;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import net.sf.cram.BLOCK_PROTO;
import net.sf.cram.CramRecord;
import net.sf.cram.ReadWrite;
import net.sf.cram.ReadWrite.CramHeader;
import net.sf.cram.structure.CompressionHeaderBLock;
import net.sf.cram.structure.Container;
import net.sf.cram.structure.ContainerHeaderIO;
import net.sf.cram.structure.Slice;
import net.sf.cram.structure.SliceIO;

import org.junit.Assert;
import org.junit.Test;

public class TestContainer {

	@Test
	public void test() throws IOException, IllegalArgumentException, IllegalAccessException {

		String cramPath = "/data/set1/small.cram";
		InputStream stream = getClass().getResourceAsStream(
				cramPath);

		if (stream == null)
			fail("CRAM file not found: " + cramPath);

		CramHeader cramHeader = ReadWrite.readCramHeader(stream);
		assertNotNull(cramHeader) ;
		assertNotNull(cramHeader.samFileHeader) ;
		assertEquals(cramHeader.majorVersion, 1) ;
		assertEquals(cramHeader.minorVersion, 1) ;

		Container container = new Container();
		ContainerHeaderIO chio = new ContainerHeaderIO();
		chio.readContainerHeader(container, stream);
		assertNotNull(container) ;
		System.out.println(container);
		
		CompressionHeaderBLock chb = new CompressionHeaderBLock(stream) ;
		container.h = chb.getCompressionHeader() ;
		
		assertNotNull(container.h) ;
		System.out.println(container.h);
		
		SliceIO sio = new SliceIO();
		container.slices = new Slice[container.landmarks.length] ;
		for (int s = 0; s < container.landmarks.length; s++) {
			Slice slice = new Slice();
			sio.readSliceHeadBlock(slice, stream);
			sio.readSliceBlocks(slice, true, stream);
			container.slices[s] = slice ;
		}
		
		System.out.println(container);
		
		ArrayList<CramRecord> records = new ArrayList<CramRecord>(container.nofRecords) ;
		BLOCK_PROTO.getRecords(container.h, container, cramHeader.samFileHeader, records) ;
		
		for (int i=0; i<records.size(); i++) {
			System.out.println(records.get(i).toString());
			if (i > 10) break ;
		}
	}

	
}
