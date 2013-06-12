package net.sf.cram.encoding.read_features;

import java.io.Serializable;
import java.util.Arrays;

public class HardClip implements Serializable, ReadFeature {
	public static final byte operator = 'H';

	private int position;
	private int length;

	public HardClip() {
	}

	public HardClip(int position, int len) {
		this.position = position;
		this.length = len;
	}

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
	
	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof HardClip))
			return false;

		HardClip v = (HardClip) obj;

		if (position != v.position)
			return false;
		if (length != v.length)
			return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getClass().getSimpleName() + "[");
		sb.append("position=").append(position);
		sb.append("; len=").append(length);
		sb.append("] ");
		return sb.toString();
	}
}
