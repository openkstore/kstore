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

/**
 *
 */
public class Convert {

	/**
	 * Converts a byte value to a long.
	 *
	 * @param val
	 * @return
	 */
	public static long byteToLong(byte val) {
		if (val == Byte.MIN_VALUE) {
			return Long.MIN_VALUE;
		}
		return val;
	}

	/**
	 * Converts a short value to a long.
	 *
	 * @param val
	 * @return
	 */
	public static long shortToLong(short val) {
		if (val == Short.MIN_VALUE) {
			return Long.MIN_VALUE;
		}
		return val;
	}

	/**
	 * Converts a integer value to a long.
	 *
	 * @param val
	 * @return
	 */
	public static long intToLong(int val) {
		if (val == Integer.MIN_VALUE) {
			return Long.MIN_VALUE;
		}
		return val;
	}

	public static long readLong(byte[] buf, int p) {
		return ((buf[p++] & 255L) << 56) | ((buf[p++] & 255L) << 48) | ((buf[p++] & 255L) << 40) | ((buf[p++] & 255L) << 32)
				| ((buf[p++] & 255L) << 24) | ((buf[p++] & 255L) << 16) | ((buf[p++] & 255L) << 8) | (buf[p++] & 255L);
	}

	public static int readShort(byte[] buf, int p) {
		return ((buf[p++] & 255) << 8) | (buf[p] & 255);
	}
}
