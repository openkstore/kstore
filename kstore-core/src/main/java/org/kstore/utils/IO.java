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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class IO {

	public static void save(OutputStream out, String s) throws IOException {
		if (s != null) {
			out.write(s.getBytes("UTF-8"));
		}
		out.write(0);
	}

	public static String loadString(DataInputStream in) throws IOException {
		ArrayByte buf = new ArrayByte().init(10);
		int c;
		while ((c = in.read()) != 0) {
			buf.add((byte) c);
		}
		return new String(buf.getBytes(), 0, buf.getSize(), "UTF-8");
	}

	public static void save(DataOutputStream out, ArrayInt tab) throws IOException {
		if (tab == null) {
			out.writeInt(0);
			return;
		}
		out.writeInt(tab.getSize());
		for (int n = 0; n < tab.getSize(); n++) {
			out.writeInt(tab.getInt(n));
		}
	}

	public static ArrayInt load(DataInputStream in, ArrayInt tab) throws IOException {
		ArrayInt res = (tab == null) ? new ArrayInt() : tab;
		res.setInts (load(in, res.getInts()));
		if (res.getSize() == 0) {
			res.setInts (new int[1]);
		}
		return res;
	}

	public static int[] load(DataInputStream in, int[] tab) throws IOException {
		tab = new int[in.readInt()];
		for (int n = 0; n < tab.length; n++) {
			tab[n] = in.readInt();
		}
		return tab;
	}

	public static byte[] load(DataInputStream in, byte[] tab) throws IOException {
		int lg = in.readInt();
		byte[] res = (tab == null) ? new byte[lg] : tab;
		in.readFully(res, 0, lg);
		return res;
	}

	public static void save(DataOutputStream out, byte[] tab, int length) throws IOException {
		out.writeInt(length);
		out.write(tab, 0, length);
	}

	public static int readInt(DataInputStream in) throws IOException {
		int c = in.read();
		if (c == -1) {
			return -1;
		}
		return (c << 24) | (in.read() << 16) | (in.read() << 8) | in.read();
	}

	public static void writeInt(int val, byte[] buf, int p) {
		buf[p++] = (byte) ((val >>> 24) & 0xFF);
		buf[p++] = (byte) ((val >>> 16) & 0xFF);
		buf[p++] = (byte) ((val >>> 8) & 0xFF);
		buf[p++] = (byte) ((val >>> 0) & 0xFF);
	}

	public static void writeLong(long val, byte[] buf, int p) {
		buf[p++] = (byte) ((val >>> 56) & 0xFF);
		buf[p++] = (byte) ((val >>> 48) & 0xFF);
		buf[p++] = (byte) ((val >>> 40) & 0xFF);
		buf[p++] = (byte) ((val >>> 32) & 0xFF);
		buf[p++] = (byte) ((val >>> 24) & 0xFF);
		buf[p++] = (byte) ((val >>> 16) & 0xFF);
		buf[p++] = (byte) ((val >>> 8) & 0xFF);
		buf[p++] = (byte) ((val >>> 0) & 0xFF);
	}

	public static void close(Closeable stream) {
		try {
			if (stream != null) {
				stream.close();
			}
		} catch (IOException e) {
			//Base.log(e);
		}
	}

	public static void close(Closeable[] tab) {
		if (tab != null) {
			for (Closeable stream : tab) {
				close(stream);
			}
		}
	}

}
