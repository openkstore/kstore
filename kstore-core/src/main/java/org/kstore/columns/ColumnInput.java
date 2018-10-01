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

import java.io.Closeable;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.iq80.snappy.Snappy;
import org.kstore.utils.ArrayInt;
import org.kstore.utils.Str;
import org.kstore.columns.io.ColumnPageBytesInput;

public abstract class ColumnInput implements Closeable {

	private static final Logger LOGGER = LogManager.getLogger(ColumnInput.class);

	private final ColumnPageBytesInput in;
	private final ArrayInt pos;
	/** Is the column in compress format or not. */
	private final boolean compress;
	/** Number of pages. */
	private int ipage;
	private byte[] buf = new byte[65536];
	private int ibuf;
	private int lbuf;
	/** The rows. */
	protected byte[] rows;
	/** Cursor position in row. */
	protected int p;

	public ColumnInput(ColumnPageBytesInput in, ArrayInt pos, boolean compress) {
		this.in = in;
		this.pos = pos;
		this.compress = compress;
	}

	public void newPage() throws IOException {
		final long start = System.currentTimeMillis();

		int size = pos.getInt(ipage);
		if (ibuf + size > lbuf) {
			byte[] nbuf = (size < buf.length) ? buf : new byte[size * 2];
			if (lbuf > 0) {
				System.arraycopy(buf, ibuf, nbuf, 0, lbuf - ibuf);
				ibuf = lbuf - ibuf;
				lbuf = ibuf;
			}
			buf = nbuf;
			while (lbuf < size) {
				int nb = in.readNextPage(buf, ibuf, buf.length - ibuf);
				if (nb <= 0) {
					break;
				}
				ibuf += nb;
				lbuf += nb;
			}
			ibuf = 0;
		}
		if (compress) {
			rows = Snappy.uncompress(buf, ibuf, size);
			p = 0;
		} else {
			rows = buf;
			p = ibuf;
		}
		ibuf += size;
		ipage++;

		long time = System.currentTimeMillis() - start;
		if (time < 100) {
			if (LOGGER.isTraceEnabled()) {
				// Do not log all .newPage as one query may this many times
				LOGGER.trace("Done loading a page in {} ms", time);
			}
		} else {
			if (LOGGER.isDebugEnabled()) {
				// We make easier to log not very-fast .newPage
				LOGGER.debug("Done loading a page in {} ms", time);
			}
		}
	}

	@Override
	public void close() throws IOException {
		in.closeColumn();
		buf = rows = null;
	}

	public abstract Str readStr() throws IOException;

	public abstract byte readByte() throws IOException;

	public abstract short readShort() throws IOException;

	public abstract int readInt() throws IOException;

	public abstract long readLong() throws IOException;

	public abstract float readFloat() throws IOException;

	public abstract double readDouble() throws IOException;

	public abstract void skipRow(int sizeType) throws IOException;

	public abstract int readRow(byte[] buf, int sizeType) throws IOException;

	final short getShort() {
		return (short) (((rows[p++] & 0xFF) << 8) + ((rows[p++] & 0xFF) << 0));
	}

	final int getInt() {
		return (((rows[p++] & 0xFF) << 24) + ((rows[p++] & 0xFF) << 16) + ((rows[p++] & 0xFF) << 8) + ((rows[p++] & 0xFF) << 0));
	}

	final long getLong() {
		return ((rows[p++] & 0xFFL) << 56) | ((rows[p++] & 0xFFL) << 48) | ((rows[p++] & 0xFFL) << 40) | ((rows[p++] & 0xFFL) << 32) | ((rows[p++] & 0xFFL) << 24) | ((rows[p++] & 0xFFL) << 16) | ((rows[p++] & 0xFFL) << 8) | (rows[p++] & 0xFFL);
	}

	final void getFully(byte[] b, int start, int len) {
		System.arraycopy(rows, p, b, start, len);
		p += len;
	}
}
