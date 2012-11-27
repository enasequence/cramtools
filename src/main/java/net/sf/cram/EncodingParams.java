package net.sf.cram;

import java.util.Arrays;

public class EncodingParams {

	public EncodingID id;
	public byte[] params;

	public EncodingParams(EncodingID id, byte[] params) {
		super();
		this.id = id;
		this.params = params;
	}
	
	@Override
	public String toString() {
		return id.name() + ":" + Arrays.toString(params) ;
	}

}
