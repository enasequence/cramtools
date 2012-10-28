/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram.encoding.read_features;

import java.io.Serializable;



public class SubstitutionVariation implements Serializable, ReadFeature {

	private int position;
	private byte base;
	private byte refernceBase;
	// private byte qualityScore;
	private BaseChange baseChange;

	public static final byte operator = 'S';

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

	public byte getBase() {
		return base;
	}

	public void setBase(byte base) {
		this.base = base;
	}

	public byte getRefernceBase() {
		return refernceBase;
	}

	public void setRefernceBase(byte refernceBase) {
		this.refernceBase = refernceBase;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SubstitutionVariation))
			return false;

		SubstitutionVariation v = (SubstitutionVariation) obj;

		if (position != v.position)
			return false;

		if (baseChange != null || v.baseChange != null) {
			if (baseChange != null) {
				if (!baseChange.equals(v.baseChange))
					return false;
			} else if (!v.baseChange.equals(baseChange))
				return false;
		}

		// if (qualityScore != v.qualityScore)
		// return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getClass().getSimpleName() + "[");
		sb.append("position=").append(position);
		if (baseChange != null)
			sb.append("; change=").append(baseChange.getChange());
		else {
			sb.append("; base=").append((char) base);
			sb.append("; referenceBase=").append((char) refernceBase);
		}
		// sb.append("; quality score=").append(qualityScore);
		sb.append("] ");
		return sb.toString();
	}

	// public byte getQualityScore() {
	// return qualityScore;
	// }
	//
	// public void setQualityScore(byte qualityScore) {
	// this.qualityScore = qualityScore;
	// }

	public BaseChange getBaseChange() {
		return baseChange;
	}

	public void setBaseChange(BaseChange baseChange) {
		this.baseChange = baseChange;
	}
}
