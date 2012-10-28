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
package net.sf.cram.stats;

import java.util.ArrayList;
import java.util.List;

public class NumberCodecOptimiser {

	private NumberCodecStub[] stubs;
	private long lengths[];

	public NumberCodecOptimiser() throws CramCompressionException {
		List<NumberCodecStub> list = new ArrayList<NumberCodecStub>();
		NumberCodecStub stub = null;
		for (int i = 2; i < 20; i++) {
			stub = NumberCodecFactory.createStub(EncodingAlgorithm.GOLOMB);
			stub.initFromString(i + ",0,1");
			list.add(stub);
		}

		for (int i = 1; i < 20; i++) {
			stub = NumberCodecFactory.createStub(EncodingAlgorithm.GOLOMB_RICE);
			stub.initFromString(i + ",0,1");
			list.add(stub);
		}

		stub = NumberCodecFactory.createStub(EncodingAlgorithm.GAMMA);
		stub.initFromString("1,0");
		list.add(stub);

		for (int i = 1; i < 20; i++) {
			stub = NumberCodecFactory.createStub(EncodingAlgorithm.SUBEXP);
			stub.initFromString(i + ",0,1");
			list.add(stub);
		}

		stub = NumberCodecFactory.createStub(EncodingAlgorithm.UNARY);
		stub.initFromString("0,1");
		list.add(stub);

		stubs = (NumberCodecStub[]) list.toArray(new NumberCodecStub[list.size()]);
		lengths = new long[stubs.length];
	}

	public void addValue(long value, long count) {
		for (int i = 0; i < stubs.length; i++) {
			NumberCodecStub stub = stubs[i];
			lengths[i] += stub.numberOfBits(value) * count;
		}
	}

	public NumberCodecStub getMinLengthStub() {
		int minLengtIndex = 0;
		for (int i = 1; i < stubs.length; i++) {
			if (lengths[minLengtIndex] > lengths[i])
				minLengtIndex = i;
		}

		return stubs[minLengtIndex];
	}

	public long getMinLength() {
		int minLengtIndex = 0;
		for (int i = 1; i < stubs.length; i++) {
			if (lengths[minLengtIndex] > lengths[i])
				minLengtIndex = i;
		}

		return lengths[minLengtIndex];
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

}
