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

import java.io.UnsupportedEncodingException;

public class Str implements Comparable<Str> {

	public static final Str NULL = new Str("NULL");

	private byte[] buf;
	private int start;
	private int len;

	private Str() {
	}

	public Str(byte[] buf) {
		this(buf, 0, buf.length);
	}

	public Str(byte[] buf, int start) {
		this(buf, start, -1);
	}

	public Str(byte[] buf, int start, int len) {
		this.buf = buf;
		this.start = start;
		this.len = len;
	}

	public Str(String s) {
		try {
			buf = s.getBytes("UTF-8");
			len = buf.length;
		} catch (UnsupportedEncodingException e) {
			buf = new byte[0];
		}
	}

	public Str(StringBuilder sb) {
		this(sb.toString());
	}

	public Str(ArrayByte tab) {
		this(tab.compact());
	}

	public byte[] getBuffer() {
		return buf;
	}

	public int getStart() {
		return start;
	}

	public int length() {
		if (len >= 0) {
			return len;
		}
		int p = start;
		while (p < buf.length && buf[p] != 0) {
			p++;
		}
		return p - start;
	}

	public char charAt(int n) {
		return (char) (buf[start + n] & 0xFF);
	}

	public byte byteAt(int n) {
		return buf[start + n];
	}

	@Override
	public Str clone() {
		int lg = length();
		Str cs = new Str();
		cs.buf = new byte[lg];
		System.arraycopy(buf, start, cs.buf, 0, lg);
		cs.len = lg;
		return cs;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof Str)) {
			return false;
		}
		Str s = (Str) other;
		int p = start;
		int sp = s.start;
		byte[] sbuf = s.buf;
		while (true) {
			if (p >= buf.length || buf[p] == 0 || (len != -1 && p - start >= len)) {
				if (sp >= sbuf.length || sbuf[sp] == 0 || (s.len != -1 && sp - s.start >= s.len)) {
					return true;
				}
				return false;
			}
			if (sp >= sbuf.length || sbuf[sp] == 0 || (s.len != -1 && sp - s.start >= s.len)) {
				return false;
			}
			if (buf[p] != sbuf[sp]) {
				return false;
			}
			p++;
			sp++;
		}
	}

	@Override
	public int hashCode() {
		int result = 1;
		int p = start;
		byte c;
		while (p < buf.length && (c = buf[p]) != 0 && (len == -1 || p - start < len)) {
			result = 31 * result + c;
			p++;
		}
		return result;
	}

	@Override
	public String toString() {
		try {
			return new String(buf, start, length(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
		}
		return "";
	}

	public byte[] toBytes() {
		byte[] res = new byte[length()];
		System.arraycopy(buf, start, res, 0, res.length);
		return res;
	}

	@Override
	public int compareTo(Str s) {
		return compareTo(buf, start, length(), s.buf, s.start, s.length());
	}

	public static int compareTo(byte[] buf, int start, int leftlg, byte[] sbuf, int sstart, int rightlg) {
		for (int i = 0, j = 0; i < leftlg && j < rightlg; i++, j++) {
			int a = (buf[start + i] & 0xff);
			int b = (sbuf[sstart + j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return leftlg - rightlg;
	}

	public int indexOf(char cf, int off) {
		int p = start + off;
		byte c;
		while (p < buf.length && (c = buf[p]) != 0) {
			if ((c & 0xFF) == cf) {
				return p - start;
			}
			p++;
		}
		return -1;
	}
}
