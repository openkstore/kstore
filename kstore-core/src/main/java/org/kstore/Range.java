package org.kstore;
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
 *
 * @author Olivier Pitton <olivier@openkstore.org>
 */

import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;

public class Range implements Iterable<Integer> {

	private final RoaringBitmap bitmap = new RoaringBitmap();;

	public Range(int... lines) {
		addLines(lines);
	}

	public Range(int start, int end) {
		this.bitmap.add(start, end);
	}

	public Range addLines(int... lines) {
		for (int line : lines) {
			this.bitmap.add(line);
		}
		return this;
	}

	public RoaringBitmap getBitmap() {
		return bitmap;
	}

	@Override
	public Iterator<Integer> iterator() {
		return this.bitmap.iterator();
	}
}
