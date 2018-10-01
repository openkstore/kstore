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

public class ArrayByte {

	private byte[] get;
	private int size;

	public byte[] getBytes() {
		return get;
	}

	public int getSize() {
		return size;
	}

	
	static int getCapacity(int size, int unit) {
		//return size*2;
		int mem = size * unit;
		if (mem < 1000000) {
			return size << 1;
		}
		int res = size + size / (((int) Math.floor(Math.log10(mem))) >> 1);
		return res;
	}

	public ArrayByte init(int bufsize) {
		get = new byte[bufsize];
		return this;
	}

	public void add(byte val) {
		alloc(1);
		get[size++] = val;
	}

	public void add(byte[] tab) {
		add(tab, 0, tab.length);
	}

	public void add(Str str) {
		add(str.getBuffer(), str.getStart(), str.length());
	}

	public void add(byte[] tab, int start, int length) {
		alloc(length);
		System.arraycopy(tab, start, get, size, length);
		size += length;
	}

	public void alloc(int length) {
		if (get == null) {
			get = new byte[1];
		}
		int nsize = size + length;
		if (nsize > get.length) {
			byte[] ndata = new byte[getCapacity(nsize, 1)];
			System.arraycopy(get, 0, ndata, 0, get.length);
			get = ndata;
		}
	}

	public byte[] compact() {
		if (get == null) {
			return new byte[0];
		}
		if (size == get.length) {
			return get;
		}
		byte[] ndata = new byte[size];
		System.arraycopy(get, 0, ndata, 0, size);
		get = ndata;
		return get;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ArrayByte)) {
			return false;
		}
		ArrayByte a = (ArrayByte) other;
		if (a.size != size) {
			return false;
		}
		for (int i = 0; i < size; i++) {
			if (get[i] != a.get[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = 1;
		for (int n = 0; n < size; n++) {
			result = 31 * result + get[n];
		}
		return result;
	}

	@Override
	public ArrayByte clone() {
		ArrayByte c = new ArrayByte();
		if (get != null) {
			c.get = get.clone();
		}
		c.size = size;
		return c;
	}
}
