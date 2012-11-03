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
import java.io.OutputStream;
import java.io.StringWriter;

public class DebuggingBitOuputStream implements BitOutputStream {
	private OutputStream os;
	private char opSeparator = '\n';
	private BitOutputStream delegate;

	public DebuggingBitOuputStream(OutputStream os, char opSeparator) {
		this(os, opSeparator, null);
	}

	public DebuggingBitOuputStream(OutputStream os, char opSeparator, BitOutputStream delegate) {
		this.os = os;
		this.opSeparator = opSeparator;
		this.delegate = delegate;
	}

	public static void main(String[] args) throws IOException {
		StringWriter writer = new StringWriter();
		DebuggingBitOuputStream bsos = new DebuggingBitOuputStream(System.out, '\n');
		bsos.write(false);
		bsos.write(true);
		bsos.write(1, 1);
		bsos.write(10, 8);

		System.out.println(writer.toString());
	}

	@Override
	public void write(int b, int nbits) throws IOException {
		for (int i = 0; i < nbits; i++)
			append(((b >> i) & 1) == 1);
		os.write(opSeparator);

		if (delegate != null)
			delegate.write(b, nbits);
	}

	@Override
	public void write(long b, int nbits) throws IOException {
		for (int i = 0; i < nbits; i++)
			append(((b >> i) & 1) == 1);
		os.write(opSeparator);

		if (delegate != null)
			delegate.write(b, nbits);
	}

	@Override
	public void write(byte b, int nbits) throws IOException {
		for (int i = 0; i < nbits; i++)
			append(((b >> i) & 1) == 1);
		os.write(opSeparator);

		if (delegate != null)
			delegate.write(b, nbits);
	}

	@Override
	public void write(boolean bit) throws IOException {
		append(bit);
		os.write(opSeparator);

		if (delegate != null)
			delegate.write(bit);
	}

	@Override
	public void write(boolean bit, long repeat) throws IOException {
		for (int i = 0; i < repeat; i++)
			append(bit);
		os.write(opSeparator);

		if (delegate != null)
			delegate.write(bit, repeat);
	}

	@Override
	public void flush() throws IOException {
		os.flush();

		if (delegate != null)
			delegate.flush();
	}

	@Override
	public void close() throws IOException {
		os.close();
		if (delegate != null)
			delegate.close();
	}

	private void append(boolean bit) throws IOException {
		os.write(bit ? '1' : '0');

	}

	@Override
	public int alignToByte() throws IOException {
		os.write("align".getBytes());
		os.write(opSeparator);
		if (delegate != null)
			return delegate.alignToByte();
		return 0;
	}

	@Override
	public void write(byte[] data) throws IOException {
		os.write(data);
		os.write(opSeparator);
		if (delegate != null)
			delegate.write(data);
	}

	@Override
	public void write(byte b) throws IOException {
		os.write(b);
		os.write(opSeparator);
		if (delegate != null)
			delegate.write(b);
	}
}
