package net.sf.cram;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.block.ExposedByteArrayOutputStream;

public class AutoOutputMap {
	private Map<Integer, ExposedByteArrayOutputStream> map = new HashMap<Integer, ExposedByteArrayOutputStream> () ;

	public ExposedByteArrayOutputStream get(Integer key) {
		return map.get(key);
	}

	public Set<Integer> keySet() {
		return map.keySet();
	}

	public ExposedByteArrayOutputStream getOrCreate (Integer key) {
		ExposedByteArrayOutputStream value = map.get(key) ;
		if (value == null) {
			value = new ExposedByteArrayOutputStream() ;
			map.put (key, value) ;
		}
		
		return value ;
	}
	
}
