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
package uk.ac.ebi.ena.sra.cram.io;

import java.io.IOException;

/**
 * Does nothing.
 * 
 * @author vadim
 * 
 */
public class NullBitOutputStream implements BitOutputStream {

	public final static NullBitOutputStream INSTANCE = new NullBitOutputStream();

	@Override
	public final void write(int b, int nbits) throws IOException {
	}

	@Override
	public final void write(long b, int nbits) throws IOException {
	}

	@Override
	public final void write(byte b, int nbits) throws IOException {
	}

	@Override
	public final void write(boolean bit) throws IOException {
	}

	@Override
	public final void write(boolean bit, long repeat) throws IOException {
	}

	@Override
	public final void flush() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public int alignToByte() throws IOException {
		return 0;
	}

	@Override
	public void write(byte[] data) throws IOException {
	}

	@Override
	public void write(byte b) throws IOException {
	}

}
