package net.sf.cram.encoding.read_features;

import java.io.Serializable;

public class ReadBase implements Serializable, ReadFeature {

	private int position;
	private byte base;
	private byte qualityScore;

	public static final byte operator = 'B';
	
	public ReadBase(int position, byte base, byte qualityScore) {
		this.position = position;
		this.base = base;
		this.qualityScore = qualityScore;
	}

	@Override
	public byte getOperator() {
		return operator;
	}

	@Override
	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public byte getQualityScore() {
		return qualityScore;
	}

	public void setQualityScore(byte qualityScore) {
		this.qualityScore = qualityScore;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ReadBase))
			return false;

		ReadBase v = (ReadBase) obj;

		if (position != v.position)
			return false;
		if (base != v.base)
			return false;
		if (qualityScore != v.qualityScore)
			return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getClass().getSimpleName() + "[");
		sb.append("position=").append(position);
		sb.append("; base=").appendCodePoint(base);
		sb.append("; score=").appendCodePoint(qualityScore);
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
