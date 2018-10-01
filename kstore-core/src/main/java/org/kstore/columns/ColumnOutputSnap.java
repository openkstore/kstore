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
import org.iq80.snappy.Snappy;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.kstore.columns.io.ColumnPageBytesOutput;

/**
 * A column output that allows to write compressed data.
 */
public class ColumnOutputSnap extends ColumnOutput {

	private DataOutputStream bufData;
	private ByteArrayOutputStream bufArray;

	/**
	 * Constructor.
	 * @param out
	 * @param pos
	 * @throws IOException 
	 */
	public ColumnOutputSnap(ColumnPageBytesOutput out, ArrayInt pos) throws IOException {
		super(out, pos);
		bufData = new DataOutputStream(bufArray = new ByteArrayOutputStream(1024));
	}

	@Override
	public int newPage() throws IOException {
		bufData.close();
		byte[] buf = bufArray.toByteArray();
		buf = Snappy.compress(buf);
		out.writeNextPageBytes(buf);
		pos.add(buf.length);
		bufArray.reset();
		bufData = new DataOutputStream(bufArray);
		return buf.length;
	}

	@Override
	public void close() throws IOException {
		super.close();
		bufArray = null;
		bufData = null;
	}

	@Override
	public void writeStr(String s) throws IOException {
		byte[] str = s.getBytes("UTF-8");
		bufData.writeShort(str.length);
		bufData.write(str);
	}

	@Override
	public void writeByte(int v) throws IOException {
		bufData.write(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		bufData.writeShort(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		bufData.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		bufData.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		bufData.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		bufData.writeDouble(v);
	}

	@Override
	public void writeRow(byte[] buf, int lg) throws IOException {
		bufData.write(buf, 0, lg);
	}

}
