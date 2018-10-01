/*
 * Copyright (C) 2018 Indexima
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kstore.utils;

import java.util.Arrays;

/**
 * A key defined as byte array.
 */
public class ByteKey implements Comparable<ByteKey> {

	private byte[] buf;

	public ByteKey(byte[] data) {
		buf = data;
	}

	public byte[] getBuffer() {
		return buf;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteKey)) {
			return false;
		}
		return Arrays.equals(buf, ((ByteKey) other).buf);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(buf);
	}

	@Override
	public int compareTo(ByteKey other) {
		return compareTo(buf, other.buf);
	}

	public static int compareTo(byte[] left, byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}
}
