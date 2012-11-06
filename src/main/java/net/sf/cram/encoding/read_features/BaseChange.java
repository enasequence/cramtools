package net.sf.cram.encoding.read_features;

import java.io.Serializable;


public class BaseChange implements Serializable{
	private int change;

	public BaseChange(int change) {
		this.change = change;
	}

	public BaseChange(byte from, byte to) {
		change = toInt(from, to);
	}

	public byte getBaseForReference(byte refBase) {
		return BaseTransitions.getBaseForTransition(refBase, change);
	}

	public static int toInt(byte from, byte to) {
		return BaseTransitions.getBaseTransition(from, to);
	}

	public int getChange() {
		return change;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BaseChange && ((BaseChange) obj).change == change)
			return true;
		return false;
	}
}
