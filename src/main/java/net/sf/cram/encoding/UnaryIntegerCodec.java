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
package net.sf.cram.encoding;

import java.io.IOException;

import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;

public class UnaryIntegerCodec implements BitCodec<Integer> {
	private boolean stopBit = false;
	private int offset = 0;

	public UnaryIntegerCodec() {
		this(false, 0);
	}

	public UnaryIntegerCodec(boolean stopBit, int offset) {
		this.stopBit = stopBit;
		this.offset = offset;
	}

	@Override
	public final Integer read (BitInputStream bis) throws IOException {
		int bits = 0;
		while (bis.readBit() != stopBit)
			bits++;

		return bits - offset;
	}

	@Override
	public final long write(BitOutputStream bos, Integer value) throws IOException {
		int newValue = value + offset;
		if (newValue < 0)
			throw new IllegalArgumentException("Unary codec, negative values not allowed: " + newValue);

		int bits = newValue + 1;

		bos.write(!stopBit, bits - 1);
		bos.write(stopBit, 1);

		return value + 1;
	}

	@Override
	public final long numberOfBits(Integer value) {
		return value + offset + 1;
	}

	public boolean isStopBit() {
		return stopBit;
	}

	public long getOffset() {
		return offset;
	}

	public void setStopBit(boolean stopBit) {
		this.stopBit = stopBit;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	@Override
	public Integer read(BitInputStream bis, int len) throws IOException {
		throw new RuntimeException("Not implemented.") ;
	}

}
