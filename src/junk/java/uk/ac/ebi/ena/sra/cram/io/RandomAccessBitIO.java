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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.sf.samtools.util.SeekableStream;

public class RandomAccessBitIO {

	public static void main(String[] args) throws IOException {

		byte[] data = "123".getBytes();
		SeekableByteArrayInputStream sbis = new SeekableByteArrayInputStream(data);
		DefaultBitInputStream dbis = new DefaultBitInputStream(sbis);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DefaultBitOutputStream dbos = new DefaultBitOutputStream(baos);

		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < 8; j++) {
				sbis.seek(i);
				dbis.reset();
				dbis.readBits(j);

				byte bits = (byte) dbis.getNofBufferedBits();
				switch (bits) {
				case 0:
					break;
				default:
					bits = (byte) (8 - bits);
					break;
				}

				System.out.printf("i=%d\tj=%d\tbuffered bits=%d\tbits=%d\n", i, j, dbis.getNofBufferedBits(), bits);
				dbos.write(dbis.readBit());
			}
		}
		// while (true) {
		// try {
		// dbos.write(dbis.readBit());
		// } catch (Exception e) {
		// break ;
		// }
		// }
		dbos.flush();
		System.out.println(baos.toString());

	}

	private static class SeekableByteArrayInputStream extends SeekableStream {
		private byte[] data;
		private int pos;

		public SeekableByteArrayInputStream(byte[] data) {
			super();
			this.data = data;
		}

		@Override
		public long length() {
			return data.length;
		}

		@Override
		public void seek(long position) throws IOException {
			pos = (int) position;
		}

		@Override
		public int read(byte[] buffer, int offset, int length) throws IOException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			data = null;
			pos = -1;
		}

		@Override
		public boolean eof() throws IOException {
			return pos > data.length - 1;
		}

		@Override
		public String getSource() {
			return null;
		}

		@Override
		public int read() throws IOException {
			return data[pos++];
		}

	}
}
