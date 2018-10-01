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

public class ArrayInt {

	private int[] get;
	private int size;

	public int getSize() {
		return size;
	}

	public int[] getInts() {
		return get;
	}

	public int getInt(int i) {
		return get[i];
	}

	public void setInts(int[] get) {
		this.get = get;
		this.size = get.length;
	}

	public ArrayInt init(int bufsize) {
		if (bufsize > 0) {
			get = new int[bufsize];
		}
		return this;
	}

	public void add(int val) {
		if (get == null) {
			get = new int[1];
		}
		if (size >= get.length) {
			int[] ndata = new int[ArrayByte.getCapacity(size, 4)];
			System.arraycopy(get, 0, ndata, 0, get.length);
			get = ndata;
		}
		get[size++] = val;
	}

	public int[] compact() {
		if (get == null) {
			return new int[0];
		}
		if (size == get.length) {
			return get;
		}
		int[] ndata = new int[size];
		System.arraycopy(get, 0, ndata, 0, size);
		get = ndata;
		return get;
	}

	public int indexOf(int val) {
		for (int n = 0; n < size; n++) {
			if (get[n] == val) {
				return n;
			}
		}
		return -1;
	}

	/* public void insert(int pos, int val)
	 * {
	 * add(0);
	 * for (int n=size-1; n>pos; n--)
	 * get[n] = get[n-1];
	 * get[pos] = val;
	 * } */
	public ArrayInt clone() {
		ArrayInt c = new ArrayInt();
		if (get != null) {
			c.get = get.clone();
		}
		c.size = size;
		return c;
	}

}
