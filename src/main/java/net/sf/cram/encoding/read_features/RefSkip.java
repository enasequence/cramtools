package net.sf.cram.encoding.read_features;

import java.io.Serializable;

import net.sf.samtools.CigarOperator;

public class RefSkip implements Serializable, ReadFeature{

	private int position;
	private int length;
	public static final byte operator = 'N';

	public RefSkip() {
	}

	public RefSkip(int position, int length) {
		this.position = position;
		this.length = length;
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
		if (!(obj instanceof RefSkip))
			return false;

		RefSkip v = (RefSkip) obj;

		if (position != v.position)
			return false;
		if (length != v.length)
			return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer().append((char)operator).append('@');
		sb.append(position);
		sb.append('+').append(length);
		return sb.toString();
	}
}
