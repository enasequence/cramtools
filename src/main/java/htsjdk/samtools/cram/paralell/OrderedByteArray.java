/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package htsjdk.samtools.cram.paralell;

class OrderedByteArray implements Comparable<OrderedByteArray>, IOrder {
	public byte[] bytes;
	public long order = 0;

	@Override
	public int compareTo(OrderedByteArray o) {
		return (int) (order - o.order);
	}

	@Override
	public String toString() {
		return String.format("order=%d, data length=%d", order, bytes == null ? -1 : bytes.length);
	}

	@Override
	public long order() {
		return order;
	}
}