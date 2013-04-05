package net.sf.cram;

import net.sf.cram.io.IOUtils;

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
		return id.name() + ":" + IOUtils.toHexString(params, 20);
	}

}
