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
package org.kstore.columns;

import java.io.IOException;
import org.kstore.utils.ArrayInt;
import org.kstore.utils.IO;
import org.kstore.utils.Str;
import org.kstore.columns.io.ColumnPageBytesInput;

/**
 *  A column input that allow to read long values.
 */
public class ColumnInputLong extends ColumnInput {

	private int size;
	private long min;

	public ColumnInputLong(ColumnPageBytesInput in, ArrayInt pos) throws IOException {
		super(in, pos, false);
	}

	@Override
	public void newPage() throws IOException {
		super.newPage();
		size = rows[p++];
		min = getLong();
	}

	@Override
	public Str readStr() throws IOException {
		throw new IOException("String  cannot be read from a LONG column");
	}

	@Override
	public byte readByte() throws IOException {
		return (byte) readInt();
	}

	@Override
	public short readShort() throws IOException {
		return (short) readInt();
	}

	@Override
	public int readInt() throws IOException {
		long val = readLong();
		if (val == Long.MIN_VALUE) {
			return Integer.MIN_VALUE;
		}
		return (int) val;
	}

	@Override
	public long readLong() {
		switch (size) {
			case 1:
				long val = (rows[p++] & 0xFF);
				if (val == 0xFF) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 2:
				val = ((rows[p++] & 0xFF) << 8) | (rows[p++] & 0xFF);
				if (val == 0xFFFF) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 3:
				val = (((rows[p++] & 0xFF) << 16) | ((rows[p++] & 0xFF) << 8) | (rows[p++] & 0xFF));
				if (val == 0xFFFFFF) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 4:
				val = (((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL));
				if (val == 0xFFFFFFFFL) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 5:
				val = (((rows[p++] & 0xFFL) << 32) | ((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL));
				if (val == 0xFFFFFFFFL) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 6:
				val = (((rows[p++] & 0xFFL) << 40) | ((rows[p++] & 0xFFL) << 32) | ((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL));
				if (val == 0xFFFFFFFFFFL) {
					return Long.MIN_VALUE;
				}
				return val + min;
			case 7:
				val = (((rows[p++] & 0xFFL) << 48) | ((rows[p++] & 0xFFL) << 40) | ((rows[p++] & 0xFFL) << 32) | ((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL));
				if (val == 0xFFFFFFFFFFFFL) {
					return Long.MIN_VALUE;
				}
				return val + min;
			default:
				val = (((rows[p++] & 0xFFL) << 56) | ((rows[p++] & 0xFFL) << 48) | ((rows[p++] & 0xFFL) << 40) | ((rows[p++] & 0xFFL) << 32) | ((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL));
				if (val == Long.MIN_VALUE) {
					return Long.MIN_VALUE;
				}
				return val + min;
		}
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat((int) readLong());
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public void skipRow(int sizeType) throws IOException {
		p += size;
	}

	@Override
	public int readRow(byte[] buf, int sizeType) throws IOException {
		IO.writeLong(readLong(), buf, 0);
		return 8;
	}
}
