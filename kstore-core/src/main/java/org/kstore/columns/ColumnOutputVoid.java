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
import java.io.IOException;
import org.kstore.columns.io.ColumnPageBytesOutput;

/**
 *
 * An output column that reads nothing, to be associated with the ColumnInputVoid.
 *
 */
public class ColumnOutputVoid extends ColumnOutput {

	public ColumnOutputVoid(ColumnPageBytesOutput out, ArrayInt pos) {
		super(out, pos);
	}

	@Override
	public int newPage() throws IOException {
		return 0;
	}

	@Override
	public void writeRow(byte[] buf, int lg) throws IOException {
	}

	@Override
	public void writeDouble(double v) throws IOException {
	}

	@Override
	public void writeFloat(float v) throws IOException {
	}

	@Override
	public void writeLong(long v) throws IOException {
	}

	@Override
	public void writeInt(int v) throws IOException {
	}

	@Override
	public void writeShort(int v) throws IOException {
	}

	@Override
	public void writeByte(int v) throws IOException {
	}

	@Override
	public void writeStr(String s) throws IOException {
	}

	@Override
	public void writeBytes(byte[] tab) throws IOException {
	}
}
