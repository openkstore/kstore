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
import org.kstore.utils.Convert;
import org.kstore.utils.IO;
import org.kstore.columns.io.ColumnPageBytesOutput;

/**
 * A column output that allow to write long values.
 */
public class ColumnOutputLong extends ColumnOutput {

	private long[] bufInt;
	private int irow;
	private long max = Long.MIN_VALUE;
	private long min = Long.MAX_VALUE;
	private byte[] rows;

	public ColumnOutputLong(ColumnPageBytesOutput out, ArrayInt pos, int nbRows) {
		super(out, pos);
		bufInt = new long[nbRows];
	}

	@Override
	public int newPage() throws IOException {
		int size = sizePage(min, max);
		int lg = 9 + size * irow;
		if (rows == null || rows.length != lg) {
			rows = new byte[lg];
		}
		int p = 0;
		rows[p++] = (byte) size;
		IO.writeLong(min, rows, p);
		p += 8;
		for (int n = 0; n < irow; n++) {
			p = writeVal(size, rows, p, bufInt[n], min);
		}
		out.writeNextPageBytes(rows);
		pos.add(rows.length);
		max = Long.MIN_VALUE;
		min = Long.MAX_VALUE;
		irow = 0;
		return rows.length;
	}

	@Override
	public void close() throws IOException {
		super.close();
		rows = null;
		bufInt = null;
	}

	@Override
	public void writeBytes(byte[] tab) throws IOException {
		throw new IOException("bytes[] cannot be written in a LONG column");
	}

	@Override
	public void writeStr(String s) throws IOException {
		throw new IOException("String cannot be written in a LONG column");
	}

	@Override
	public void writeByte(int v) throws IOException {
		writeLong(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		writeLong(v);
	}

	@Override
	public void writeInt(int id) {
		if (id == Integer.MIN_VALUE) {
			writeLong(Long.MIN_VALUE);
		} else {
			writeLong(id);
		}
	}

	@Override
	public void writeLong(long id) {
		bufInt[irow++] = id;
		if (id == Long.MIN_VALUE) {
			return;
		}
		if (id > max) {
			max = id;
		}
		if (id < min) {
			min = id;
		}
	}

	@Override
	public void writeFloat(float v) throws IOException {
		writeLong(Float.floatToRawIntBits(v));
	}

	@Override
	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToRawLongBits(v));
	}

	@Override
	public void writeRow(byte[] buf, int lg) {
		writeLong(Convert.readLong(buf, 0));
	}

	static final int sizePage(long min, long max) {
		long dec = max - min;
		int size = 8;
		if (dec < 0xFFL) {
			size = 1;
		} else if (dec < 0xFFFFL) {
			size = 2;
		} else if (dec < 0xFFFFFFL) {
			size = 3;
		} else if (dec < 0xFFFFFFFFL) {
			size = 4;
		} else if (dec < 0xFFFFFFFFFFL) {
			size = 5;
		} else if (dec < 0xFFFFFFFFFFFFL) {
			size = 6;
		} else if (dec < 0xFFFFFFFFFFFFFFL) {
			size = 7;
		}
		return size;
	}

	static final int writeVal(int size, byte[] rows, int p, long val, long min) {
		long value = (val == Long.MIN_VALUE) ? 0xFFFFFFFFFFFFFFFFL : val - min;
		switch (size) {
			case 1:
				rows[p++] = (byte) value;
				break;
			case 2:
				rows[p++] = (byte) ((value & 0xFF00) >> 8);
				rows[p++] = (byte) (value & 0x00FF);
				break;
			case 3:
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
			case 4:
				rows[p++] = (byte) ((value & 0xFF000000) >> 24);
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
			case 5:
				rows[p++] = (byte) ((value & 0xFF00000000L) >> 32);
				rows[p++] = (byte) ((value & 0xFF000000) >> 24);
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
			case 6:
				rows[p++] = (byte) ((value & 0xFF0000000000L) >> 40);
				rows[p++] = (byte) ((value & 0xFF00000000L) >> 32);
				rows[p++] = (byte) ((value & 0xFF000000) >> 24);
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
			case 7:
				rows[p++] = (byte) ((value & 0xFF000000000000L) >> 48);
				rows[p++] = (byte) ((value & 0xFF0000000000L) >> 40);
				rows[p++] = (byte) ((value & 0xFF00000000L) >> 32);
				rows[p++] = (byte) ((value & 0xFF000000) >> 24);
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
			case 8:
				value = (val == Long.MIN_VALUE) ? Long.MIN_VALUE : val - min;
				rows[p++] = (byte) ((value & 0xFF00000000000000L) >> 56);
				rows[p++] = (byte) ((value & 0xFF000000000000L) >> 48);
				rows[p++] = (byte) ((value & 0xFF0000000000L) >> 40);
				rows[p++] = (byte) ((value & 0xFF00000000L) >> 32);
				rows[p++] = (byte) ((value & 0xFF000000) >> 24);
				rows[p++] = (byte) ((value & 0xFF0000) >> 16);
				rows[p++] = (byte) ((value & 0x00FF00) >> 8);
				rows[p++] = (byte) (value & 0x0000FF);
				break;
		}
		return p;
	}
}
