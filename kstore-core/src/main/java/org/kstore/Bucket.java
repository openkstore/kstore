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
package org.kstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.utils.BitmapLong;
import org.kstore.utils.ByteKey;
import org.kstore.utils.Convert;
import org.kstore.utils.IO;
import org.roaringbitmap.RoaringBitmap;

public abstract class Bucket implements Comparable<ByteKey> {
	
	protected static final String ID = "id";
	protected static final String COL = "col";
	protected static final String ADD = "_add";

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(Bucket.class);
	/** Buckets are expected in a given folder. */
	protected String directory;
	/** Most operations will rely on a relative path. */
	protected String path;
	/** The store owning this bucket. */
	protected KStore store;

	protected ByteKey minRow;
	protected long size;
	protected long sizeCommit;
	protected long count;
	protected long countCommit;
	protected long countDelete;
	private DataOutputStream[] out;
	protected boolean modified = false;
	private boolean needMerge = false;
	protected BitmapLong deleteRowNums;
	protected BitmapLong deleteRowIds;

	public static class BucketLoader {

		public void load(int rowNum, int rowId, byte[][] buf, int[] size) throws IOException {
		}

		public void loadOne(int rowNum, int rowId, byte[] buf, int size) throws IOException {
		}
	}

	public static interface LineReader {

		boolean readNext(int rowId, Line line) throws IOException;
	}

	public Bucket(KStore store) {
		this.store = store;
		minRow = new ByteKey("".getBytes());
	}

	public Bucket init(String directory, String path) {
		this.directory = directory;
		this.path = path;
		return this;
	}

	protected void init(Bucket other) {
		directory = other.directory;
		path = other.path;
		minRow = other.minRow;
		size = other.size;
		sizeCommit = other.sizeCommit;
		count = other.count;
		countCommit = other.countCommit;
	}

	@Override
	public int compareTo(ByteKey other) {
		return minRow.compareTo(other);
	}

	private String getAbsolutePath() {
		return makeAbsolutePath(path);
	}

	private String makeAbsolutePath(String subPath) {
		if (directory == null) {
			throw new IllegalStateException("directory has not been initialized");
		}
		if (subPath == null) {
			throw new IllegalStateException("path may not be null");
		}
		return directory + subPath;
	}

	String getColPath(String name) {
		return getAbsolutePath() + "/" + name;
	}

	private DataInputStream[] openReadOne(String post, int icol) throws IOException {
		DataInputStream[] in = new DataInputStream[1 + 1];
		in[0] = store.getDevice().open(getColPath(ID) + post, Configuration.getCompressionType());
		in[1] = store.getDevice().open(getColPath(COL + icol + post), Configuration.getCompressionType());
		return in;
	}

	private DataInputStream[] openRead(String post) throws IOException {
		DataInputStream[] in = new DataInputStream[store.getNumberOfColumns() + 1];
		in[0] = store.getDevice().open(getColPath(ID) + post, Configuration.getCompressionType());
		for (int n = 1; n < in.length; n++) {
			in[n] = store.getDevice().open(getColPath(COL + (n - 1) + post), Configuration.getCompressionType());
		}
		return in;
	}

	private DataOutputStream[] openWrite(String post) throws IOException {
		DataOutputStream[] dOut = new DataOutputStream[store.getNumberOfColumns() + 1];
		dOut[0] = store.getDevice().create(getColPath(ID + post), Configuration.getCompressionType(), false);
		for (int n = 1; n < dOut.length; n++) {
			dOut[n] = store.getDevice().create(getColPath(COL + (n - 1) + post), Configuration.getCompressionType(), false);
		}
		return dOut;
	}

	private DataOutputStream[] openWriteOne(String post, int icol) throws IOException {
		DataOutputStream[] dOut = new DataOutputStream[1 + 1];
		if (icol == 0) {
			dOut[0] = store.getDevice().create(getColPath(ID + post), Configuration.getCompressionType(), false);
		}
		dOut[1] = store.getDevice().create(getColPath(COL + (icol) + post), Configuration.getCompressionType(), false);
		return dOut;
	}

	private void rename(String post1, String post2) throws IOException {
		store.getDevice().rename(getColPath(ID + post1), getColPath(ID + post2));
		for (int n = 0; n < store.getNumberOfColumns(); n++) {
			store.getDevice().rename(getColPath(COL + n + post1), getColPath(COL + n + post2));
		}
	}

	private void delete(String post) throws IOException {
		store.getDevice().delete(getColPath(ID + post));
		for (int n = 0; n < store.getNumberOfColumns(); n++) {
			store.getDevice().delete(getColPath(COL + n + post));
		}
	}

	private void initOut(String post) throws IOException {
		out = openWrite(post);
	}

	public void add(int keyId, Object[] values) throws IOException {
		if (out == null) {
			if (size == 0) {
				initOut("");
			} else {
				initOut(ADD);
				needMerge = true;
			}
		}
		out[0].writeInt(keyId);
		for (int n = 1; n < values.length + 1; n++) {
			Object val = values[n - 1];
			Column fi = store.getColumn(n - 1);
			switch (fi.getColumnType()) {
				case TINYINT:
					out[n].writeByte((Byte) val);
					break;
				case SMALLINT:
					out[n].writeShort((Short) val);
					break;
				case INT:
					out[n].writeInt((Integer) val);
					break;
				case FLOAT:
					out[n].writeFloat((Float) val);
					break;
				case BIGINT:
					out[n].writeLong((Long) val);
					break;
				case DOUBLE:
					out[n].writeDouble((Double) val);
					break;
				case STRING:
				case TIMESTAMP:
				case DATE:
					byte[] str = val.toString().getBytes("UTF-8");
					out[n].writeShort(str.length);
					out[n].write(str);
					size += str.length + 2;
					break;
			}
			size += fi.getSize();
		}
		count++;
	}

	public void deleteRowNum(long numLine) {
		if (deleteRowNums == null) {
			deleteRowNums = new BitmapLong();
		}
		deleteRowNums.add(numLine);
		countDelete = countCommit;
	}

	public void deleteRowId(long rowId) {
		if (deleteRowIds == null) {
			deleteRowIds = new BitmapLong();
		}
		deleteRowIds.add(rowId);
		countDelete = countCommit;
	}

	protected void close() throws IOException {
		if (out != null) {
			if (needMerge) {
				if (deleteRowIds == null && deleteRowNums == null) {
					addStream(openRead(""), out);
				} else {
					compactTo("", this);
				}
			}
			for (DataOutputStream o : out) {
				if (o != null) {
					o.close();
				}
			}
			out = null;
			modified = true;
		}
		initCommitValues();
	}

	public void commit() throws IOException {
		close();
		if (needMerge) {
			delete("");
			rename(ADD, "");
		}
		needMerge = false;
		compact();
	}

	public void rollback() throws IOException {
		LOGGER.info("Rollbacking '" + path + "' ...");
		count = countCommit;
		size = sizeCommit;
		if (out != null) {
			IO.close(out);
			out = null;
			if (needMerge) {
				delete(ADD);
			} else {
				delete("");
			}
		}
		needMerge = false;
		deleteRowNums = deleteRowIds = null;
	}

	protected void initCommitValues() throws IOException {
		countCommit = count;
		sizeCommit = size;
	}

	private void load(String post, BucketLoader loader) throws IOException {
		int columns = store.getNumberOfColumns();
		DataInputStream[] in = openRead(post);
		byte[][] buf = new byte[columns][];
		int[] cSizes = new int[columns];
		for (int n = 0; n < columns; n++) {
			buf[n] = new byte[(store.getColumn(n).getSize() == 0) ? Short.MAX_VALUE + 2 : 8];
		}
		int rowId;
		int rowNum = 0;
		while ((rowId = IO.readInt(in[0])) != -1) {
			for (int n = 0; n < columns; n++) {
				cSizes[n] = store.getColumn(n).getSize();
				if (cSizes[n] == 0) {
					in[n + 1].readFully(buf[n], 0, 2);
					cSizes[n] = Convert.readShort(buf[n], 0);
					in[n + 1].readFully(buf[n], 2, cSizes[n]);
					cSizes[n] += 2;
				} else {
					in[n + 1].readFully(buf[n], 0, cSizes[n]);
				}
			}
			loader.load(rowNum++, rowId, buf, cSizes);
		}
		for (DataInputStream istream : in) {
			istream.close();
		}
	}

	void loadOne(String post, int icol, BucketLoader loader) throws IOException {
		DataInputStream[] in = openReadOne(post, icol);
		byte[][] buf = new byte[1][];
		int[] cSizes = new int[1];
		buf[0] = new byte[(store.getColumn(icol).getSize() == 0) ? Short.MAX_VALUE + 2 : 8];
		int rowId;
		int rowNum = 0;
		while ((rowId = IO.readInt(in[0])) != -1) {
			cSizes[0] = store.getColumn(icol).getSize();
			if (cSizes[0] == 0) {
				in[1].readFully(buf[0], 0, 2);
				cSizes[0] = Convert.readShort(buf[0], 0);
				in[1].readFully(buf[0], 2, cSizes[0]);
				cSizes[0] += 2;
			} else {
				in[1].readFully(buf[0], 0, cSizes[0]);
			}

			loader.load(rowNum++, rowId, buf, cSizes);
		}
		for (DataInputStream istream : in) {
			istream.close();
		}
	}

	abstract public void readLines(Line line, int[] columns, RoaringBitmap bitRowIds, LineReader liner) throws IOException;

	void moveTo(String newPath) throws IOException {
		LOGGER.info("Moving " + path + " to " + newPath);
		store.getDevice().rename(makeAbsolutePath(path), makeAbsolutePath(newPath));
		path = newPath;
	}

	int[] getAllColIds() {
		int[] colIds = new int[store.getNumberOfColumns()];
		for (int n = 0; n < colIds.length; n++) {
			colIds[n] = n;
		}
		return colIds;
	}

	private static long div(long num, long den) {
		long res = num / den;
		if (num % den > 0) {
			res++;
		}
		return res;
	}

	protected void compact() throws IOException {
		if (deleteRowNums == null && deleteRowIds == null) {
			return;
		}
		final Bucket buc = store.newBucket().init(directory, path + "_compact");
		buc.minRow = minRow;
		buc.initOut("");
		compactTo("", buc);
		buc.close();
		drop();
		buc.moveTo(getAbsolutePath());
		init(buc);
	}

	private void compactTo(String post, final Bucket buc) throws IOException {
		long time = System.currentTimeMillis();
		load(post, new BucketLoader() {
			@Override
			public void load(int rowNum, int rowId, byte[][] buf, int[] size) throws IOException {
				if (deleteRowNums != null && deleteRowNums.contains(rowNum)) {
					return;
				}
				if (deleteRowIds != null && deleteRowIds.contains(rowId)) {
					return;
				}
				buc.out[0].writeInt(rowId);
				buc.count++;
				for (int n = 0; n < store.getNumberOfColumns(); n++) {
					buc.out[n + 1].write(buf[n], 0, size[n]);
					buc.size += size[n];
				}
			}
		});
		deleteRowNums = deleteRowIds = null;
		LOGGER.info("Compacting bucket " + path + " in " + (System.currentTimeMillis() - time) + " ms.");
	}

	private void addStream(InputStream[] in, OutputStream[] out) throws IOException {
		byte[] buf = new byte[64000];
		for (int n = 0; n < in.length; n++) {
			int nb;
			while ((nb = in[n].read(buf)) > 0) {
				out[n].write(buf, 0, nb);
			}
			in[n].close();
		}
	}

	public void drop() throws IOException {
		String absolutePath = getAbsolutePath();
		LOGGER.info("Deleting " + absolutePath);
		for (int n = 0; n < store.getNumberOfColumns(); n++) {
			store.getDevice().delete(getColPath(COL + n));
		}
		store.getDevice().delete(getColPath(ID));
		store.getDevice().delete(absolutePath);
	}

	public void save(DataOutputStream indexOut) throws IOException {
		// We save only the relative path in files. This way, files can be copied into a different root folder
		// Replace \ windows with / in order to maintain compatibility
		IO.save(indexOut, path.replaceAll("\\\\", "/"));
		IO.save(indexOut, minRow.getBuffer(), minRow.getBuffer().length);
		indexOut.writeLong(size);
		indexOut.writeLong(count);
	}

	public Bucket load(String directory, DataInputStream stream) throws IOException {
		this.directory = directory;
		path = IO.loadString(stream);
		minRow = new ByteKey(IO.load(stream, (byte[]) null));
		size = stream.readLong();
		count = stream.readLong();
		initCommitValues();
		return this;
	}

}
