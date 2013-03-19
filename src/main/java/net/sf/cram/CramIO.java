package net.sf.cram;

import java.io.InputStream;
import java.util.List;

import net.sf.cram.index.CramIndex;
import net.sf.cram.structure.Container;
import net.sf.samtools.SAMRecord;

public abstract class CramIO {
	
	protected abstract void containerRead (Container c) ;
	protected abstract void cramRecordsParsed (List<CramRecord> cramRecords) ;
	protected abstract void cramRecordsNormilized(List<CramRecord> cramRecords) ;
	protected abstract void samRecordsCreated(List<SAMRecord> samRecords) ;
	
	public void  readNextContainer(InputStream is) {
	}
	
	public void position (CramIndex.Entry entry) {
	}
}
