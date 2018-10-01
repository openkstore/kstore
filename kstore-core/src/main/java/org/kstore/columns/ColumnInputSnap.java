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
import org.kstore.utils.Str;
import org.kstore.columns.io.ColumnPageBytesInput;

/**
 * A column input that allows to read compressed data.
 */
public class ColumnInputSnap extends ColumnInput {

	public ColumnInputSnap(ColumnPageBytesInput in, ArrayInt pos) throws IOException {
		super(in, pos, true);
	}

	@Override
	public Str readStr() throws IOException {
		int len = getShort();
		byte[] buf = new byte[len];
		getFully(buf, 0, len);
		return new Str(buf, 0, len);
	}

	@Override
	public byte readByte() throws IOException {
		return rows[p++];
	}

	@Override
	public short readShort() throws IOException {
		return getShort();
	}

	@Override
	public int readInt() throws IOException {
		return getInt();
	}

	@Override
	public long readLong() throws IOException {
		return getLong();
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(getInt());
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(getLong());
	}

	@Override
	public void skipRow(int sizeType) throws IOException {
		if (sizeType == 0) {
			sizeType = getShort();
		}
		p += sizeType;
	}

	@Override
	public int readRow(byte[] buf, int sizeType) throws IOException {
		if (sizeType == 0) {
			getFully(buf, 0, 2);
			sizeType = Convert.readShort(buf, 0);
			getFully(buf, 2, sizeType);
			sizeType += 2;
		} else {
			getFully(buf, 0, sizeType);
		}
		return sizeType;
	}
}
