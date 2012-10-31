package net.sf.cram.encoding.read_features;

import java.io.Serializable;

import net.sf.samtools.CigarOperator;

public class SoftClipVariation implements Serializable, ReadFeature{

	private int position;
	private int length;

	public SoftClipVariation() {
	}

	public SoftClipVariation(int position, int length) {
		this.position = position;
		this.length = length;
	}

	public static final byte operator = CigarOperator.enumToCharacter(CigarOperator.S);

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
		if (!(obj instanceof SoftClipVariation))
			return false;

		SoftClipVariation v = (SoftClipVariation) obj;

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
		sb.append("; length=").append(length);
		sb.append("] ");
		return sb.toString();
	}
}
