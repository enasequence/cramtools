package net.sf.cram.encoding.read_features;

import java.io.Serializable;

public class InsertBase implements Serializable, ReadFeature {

	private int position;
	private byte base;

	public InsertBase() {
	}

	public InsertBase(int position, byte base) {
		this.position = position;
		this.base = base;
	}

	public static final byte operator = 'i';

	@Override
	public byte getOperator() {
		return operator;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof InsertBase))
			return false;

		InsertBase v = (InsertBase) obj;

		if (position != v.position)
			return false;

		if (base != v.base)
			return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getClass().getSimpleName() + "[");
		sb.append("position=").append(position);
		sb.append("; base=").appendCodePoint(base);
		sb.append("] ");
		return sb.toString();
	}

	public byte getBase() {
		return base;
	}

	public void setBase(byte base) {
		this.base = base;
	}
}
