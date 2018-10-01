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

import org.kstore.utils.ArrayInt;
import org.kstore.utils.Str;
import java.io.IOException;
import org.kstore.columns.io.ColumnPageBytesInput;

/**
 * A column input that acts as a 'void' column, e.g. will store nothing.
 *
 */
public class ColumnInputVoid extends ColumnInput {

	public ColumnInputVoid(ColumnPageBytesInput in, ArrayInt pos) {
		super(in, pos, false);
	}

	@Override
	public int readRow(byte[] buf, int sizeType) throws IOException {
		return 0;
	}

	@Override
	public void skipRow(int sizeType) throws IOException {
	}

	@Override
	public double readDouble() throws IOException {
		return Double.NaN;
	}

	@Override
	public float readFloat() throws IOException {
		return Float.NaN;
	}

	@Override
	public long readLong() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public int readInt() throws IOException {
		return Integer.MAX_VALUE;
	}

	@Override
	public short readShort() throws IOException {
		return Short.MAX_VALUE;
	}

	@Override
	public byte readByte() throws IOException {
		return Byte.MAX_VALUE;
	}

	@Override
	public Str readStr() throws IOException {
		return Str.NULL;
	}

}
