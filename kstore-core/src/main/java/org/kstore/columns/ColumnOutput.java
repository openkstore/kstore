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
import org.kstore.columns.io.ColumnPageBytesOutput;

/**
 * 
 */
public abstract class ColumnOutput {

	protected ColumnPageBytesOutput out;
	protected ArrayInt pos;

	public ColumnOutput(ColumnPageBytesOutput out, ArrayInt pos) {
		this.out = out;
		this.pos = pos;
	}

	public abstract int newPage() throws IOException;

	public void close() throws IOException {
		out.closeColumn();
		pos.compact();
	}

	public void writeBytes(byte[] tab) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeStr(String s) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeByte(int v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeShort(int v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeInt(int v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeLong(long v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeFloat(float v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeDouble(double v) throws IOException {
		throw new IOException("Data Type not implemented");
	}

	public void writeRow(byte[] buf, int lg) throws IOException {
		throw new IOException("Data Type not implemented");
	}
}
