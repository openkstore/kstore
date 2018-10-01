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

import java.util.HashMap;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

/**
 * 
 */
public class BitmapLong {

	private Map<Integer, RoaringBitmap> hbits = new HashMap<>();

	public void add(long val) {
		int high = getHigh(val);
		RoaringBitmap bits = hbits.get(high);
		if (bits == null) {
			hbits.put(high, bits = new RoaringBitmap());
		}
		bits.add(getLow(val));
	}

	public boolean contains(long val) {
		RoaringBitmap bits = hbits.get(getHigh(val));
		if (bits == null) {
			return false;
		}
		return bits.contains(getLow(val));
	}

	static int getHigh(long val) {
		return (int) (val >> 32);
	}

	static int getLow(long val) {
		return (int) (val & 0xFFFFFFFF);
	}
}
