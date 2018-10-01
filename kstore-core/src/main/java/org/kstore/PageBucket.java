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

import org.kstore.columns.io.MultipleInputStream;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.columns.ColumnInput;
import org.kstore.columns.ColumnInputLong;
import org.kstore.columns.ColumnInputSnap;
import org.kstore.columns.ColumnInputVoid;
import org.kstore.columns.ColumnOutput;
import org.kstore.columns.ColumnOutputLong;
import org.kstore.columns.ColumnOutputSnap;
import org.kstore.columns.ColumnOutputVoid;
import org.kstore.columns.io.MultipleColumnPageBytesInput;
import org.kstore.columns.io.MultipleColumnPageBytesOutput;
import org.kstore.columns.io.SingleColumnPageBytesInput;
import org.kstore.columns.io.SingleColumnPageBytesOutput;
import org.kstore.utils.BucketIOSharedPool;
import org.kstore.utils.IO;
import org.kstore.utils.Convert;
import org.roaringbitmap.RoaringBitmap;
import org.kstore.columns.io.ColumnPageBytesInput;
import org.kstore.columns.io.ColumnPageBytesOutput;
import org.kstore.columns.io.MultiInputStream;

/**
 *
 */
public class PageBucket extends Bucket {

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(PageBucket.class);

	private static final int FAST_LOAD_TIME = 100;
	private static final int LOAD_TIME = 5000;

	// Used to seggregate between the id column and the data columns
	private static final int ID_COL_INDEX = Integer.MIN_VALUE;

	// Used to mark files which holds all data columns
	private static final String TAG_SINGLE_FILE_ALL_COLUMNS = "ALL";

	private ColumnOutput[] out;
	// Number of rows pending in current page
	private int countPage;
	private int sizePage = Configuration.getNbPages();
	private List<RowFile> rowFiles;
	private int rowFilesCommit;
	private RowFile rowFile;
	private byte[] colFormat;
	private boolean oneFilePerColumn;

	public PageBucket(KStore store) {
		super(store);
		colFormat = new byte[store.getNumberOfColumns()];
		rowFiles = new ArrayList<>();
		oneFilePerColumn = store.useOneFilePerColumn();
	}

	private void init(PageBucket other) {
		super.init(other);
		sizePage = other.sizePage;
		rowFiles = other.rowFiles;
		colFormat = other.colFormat;
		rowFilesCommit = other.rowFilesCommit;
		oneFilePerColumn = other.oneFilePerColumn;
	}

	private void initFormat() {
		for (int n = 0; n < store.getNumberOfColumns(); n++) {
			if (colFormat[n] == 0) {
				Column fi = store.getColumn(n);
				ColumnType t = fi.getColumnType();
				if (t == ColumnType.TINYINT || t == ColumnType.SMALLINT || t == ColumnType.INT || t == ColumnType.BIGINT) {
					colFormat[n] = (byte) 2;
				} else {
					colFormat[n] = (byte) 1;
				}
			}
		}
	}

	private ColumnOutput[] openWriteCol(RowFile rf, int[] colIds, boolean withId) throws IOException {
		initFormat();
		ColumnOutput[] cOuts = new ColumnOutput[1 + colIds.length];

		final IntFunction<ColumnPageBytesOutput> columnIdToByteConsumer;
		if (oneFilePerColumn) {
			// Each column is written in its own file
			columnIdToByteConsumer = i -> new SingleColumnPageBytesOutput(oneFilePerColumnOutputStream(rf, i));
		} else {
			// We consider a single outputStream for all data columns
			MultipleColumnPageBytesOutput allDataColumns = new MultipleColumnPageBytesOutput(
					makeOutputStream(COL + TAG_SINGLE_FILE_ALL_COLUMNS, rf));

			columnIdToByteConsumer = i -> {
				allDataColumns.incrementNbColumns();
				return allDataColumns;
			};
		}

		// We keep the id column in its own column
		if (withId) {
			cOuts[0] = new ColumnOutputLong(new SingleColumnPageBytesOutput(oneFilePerColumnOutputStream(rf, ID_COL_INDEX)), rf.getPos(0), sizePage);
		}
		for (int n = 1; n < cOuts.length; n++) {
			int colId = colIds[n - 1];
			Column fi = store.getColumn(colId);
			//Dico dico = table.vtable.getDico(fi.name, false);
			if (fi.isCalculated()) {
				// calculated fields are not stored. the 'void' column does nothing.
				cOuts[n] = new ColumnOutputVoid(columnIdToByteConsumer.apply(colId), rf.getPos(1 + colId));
			} else {
				if (colFormat[colId] == 2) {
					cOuts[n] = new ColumnOutputLong(columnIdToByteConsumer.apply(colId), rf.getPos(1 + colId), sizePage);
				} else {
					cOuts[n] = new ColumnOutputSnap(columnIdToByteConsumer.apply(colId), rf.getPos(1 + colId));
				}
			}
		}
		return cOuts;
	}

	private OutputStream oneFilePerColumnOutputStream(RowFile rf, int colId) {
		try {
			if (colId == ID_COL_INDEX) {
				return makeOutputStream(ID, rf);
			} else {
				return makeOutputStream(COL + colId, rf);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private OutputStream makeOutputStream(String prefix, RowFile rf) throws IOException {
		return store.getDevice().getOutputStream(getColPath(prefix + rf.getPost()), false);
	}

	private MultiInputStream openReadCol(RowFile rf) throws IOException {
		return openReadCol(rf, getAllColIds());
	}

	private MultiInputStream openReadCol(RowFile rf, int[] colIds) throws IOException {
		ColumnInput[] in = new ColumnInput[1 + colIds.length];
		in[0] = new ColumnInputLong(new SingleColumnPageBytesInput(store.getDevice().getInputStream(getColPath(ID + rf.getPost()))), rf.getPos(0));

		boolean openFilesConcurrently;

		final IntFunction<ColumnPageBytesInput> pageBytesProvider;
		if (oneFilePerColumn) {
			// There is one InputStream per column
			pageBytesProvider = i -> new SingleColumnPageBytesInput(readColumn(rf, i));
			openFilesConcurrently = true;
		} else {
			// We open a single inputStream shared between all columns
			MultipleColumnPageBytesInput singleFileProvider = new MultipleColumnPageBytesInput(rf.getPos(), store.getDevice().getInputStream(getColPath(COL + TAG_SINGLE_FILE_ALL_COLUMNS + rf.getPost())));
			pageBytesProvider = i -> singleFileProvider.popColumnReader(i);
			openFilesConcurrently = false;
		}

		List<ColumnInput> asList = openColumns(openFilesConcurrently, rf, colIds, in, pageBytesProvider);

		for (int n = 1; n < in.length; n++) {
			in[n] = asList.get(n - 1);
		}
		return new MultipleInputStream(openFilesConcurrently, in);
	}

	private List<ColumnInput> openColumns(boolean openFilesConcurrently,
			RowFile rf,
			int[] colIds,
			ColumnInput[] in,
			final IntFunction<ColumnPageBytesInput> pageBytesProvider) {
		// We may open data column in parallel
		List<ListenableFuture<ColumnInput>> futureInputs = IntStream.range(1, in.length).mapToObj(n -> {
			if (openFilesConcurrently) {
				return BucketIOSharedPool.submit(() -> {
					return openOneColumn(rf, colIds, pageBytesProvider, n);
				});
			} else {
				try {
					return Futures.immediateFuture(openOneColumn(rf, colIds, pageBytesProvider, n));
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
		}).collect(Collectors.toList());

		return Futures.getUnchecked(Futures.allAsList(futureInputs));
	}

	private ColumnInput openOneColumn(RowFile rf, int[] colIds,
			final IntFunction<ColumnPageBytesInput> pageBytesProvider, int n) throws IOException {
		int colId = colIds[n - 1];
		Column fi = store.getColumn(colId);
		//Dico dico = table.vtable.getDico(fi.name, false);
		if (fi.isCalculated()) {
			// calculated fields are not stored. the 'void' column does nothing.
			return new ColumnInputVoid(pageBytesProvider.apply(colId), rf.getPos(1 + colId));
		}
		if (colFormat[colId] == 2) {
			return new ColumnInputLong(pageBytesProvider.apply(colId), rf.getPos(1 + colId));
		} else {
			return new ColumnInputSnap(pageBytesProvider.apply(colId), rf.getPos(1 + colId));
		}
	}

	private InputStream readColumn(RowFile rf, int colId) {
		try {
			return store.getDevice().getInputStream(getColPath(COL + colId + rf.getPost()));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void initOut() throws IOException {
		initOut(getAllColIds(), true);
	}

	private void initOut(int[] colIds, boolean withId) throws IOException {
		countPage = 0;
		if (withId) {
			rowFiles.add(rowFile = new RowFile(store.getNumberOfColumns()).init("_" + rowFiles.size(), count));
		}
		out = openWriteCol(rowFile, colIds, withId);
	}

	@Override
	public void add(int keyId, Object[] values) throws IOException {
		if (out == null) {
			initOut();
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
					out[n].writeStr(val.toString());
					break;
			}
		}
		addRow();
	}

	private void addRow() throws IOException {
		if (++countPage >= sizePage) {
			newPage();
		}
	}

	private void newPage() throws IOException {
		for (ColumnOutput colOut : out) {
			if (colOut != null) {
				size += colOut.newPage();
			}
		}
		count += countPage;
		rowFile.getPosCount().add(countPage);
		countPage = 0;
	}

	@Override
	public void readLines(Line line, int[] columns, RoaringBitmap bitRowIds, LineReader liner) throws IOException {
		if (countCommit == 0) {
			return;
		}
		int[] indexInOriginal = computerIndexOfSorted(columns);

		long count = 0;
		long total = 0;
		long min = Long.MAX_VALUE;
		long max = 0;
		long countB = 0;
		long totalB = 0;
		long minB = Long.MAX_VALUE;
		long maxB = 0;

		loop:
		for (int ifile = 0; ifile < rowFilesCommit; ifile++) {
			RowFile rf = rowFiles.get(ifile);

			try (MultiInputStream in = openReadCol(rf, columns)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Reading " + ifile + " " + rf.getPosCount().getSize());
				}
				for (int ipage = 0; ipage < rf.getPosCount().getSize(); ipage++) {

					// We need to process column in growing order, as if columns are in a single file, we can load column content only in growing order
					long start = System.nanoTime();
					for (int columnIndex : indexInOriginal) {
						ColumnInput col = in.getColumn(columnIndex);
						loadNextPage(indexInOriginal, rf, ipage, columnIndex, col);
					}
					long stop = System.nanoTime();
					long diff = (stop - start);
					count += 1;
					total += diff;
					min = (diff < min) ? diff : min;
					max = (diff > max) ? diff : max;

					start = System.nanoTime();
					for (int irow = 0; irow < rf.getPosCount().getInt(ipage); irow++) {
						int rowId = in.getColumn(0).readInt();
						if (bitRowIds != null && !bitRowIds.contains(rowId)) {
							for (int n = 1; n < in.getColumnCount(); n++) {
								in.getColumn(n).skipRow(store.getColumn(n - 1).getSize());
							}
							continue;
						}
						for (int n = 1; n < in.getColumnCount(); n++) {
							int colId = columns[n - 1];
							ColumnInput is = in.getColumn(n);

							switch (store.getColumn(colId).getColumnType()) {
								case TINYINT:
									line.addLong(colId, Convert.byteToLong(is.readByte()));
									break;
								case SMALLINT:
									line.addLong(colId, Convert.shortToLong(is.readShort()));
									break;
								case INT:
									line.addLong(colId, Convert.intToLong(is.readInt()));
									break;
								case FLOAT:
									line.addDouble(colId, is.readFloat());
									break;
								case BIGINT:
									line.addLong(colId, is.readLong());
									break;
								case DOUBLE:
									line.addDouble(colId, is.readDouble());
									break;
								case STRING:
								case TIMESTAMP:
								case DATE:
									line.addString(colId, is.readStr());
									break;
							}
						}
						if (!liner.readNext(rowId, line)) {
							break loop;
						}
					}
					stop = System.nanoTime();
					diff = (stop - start);
					countB += 1;
					totalB += diff;
					minB = (diff < min) ? diff : min;
					maxB = (diff > max) ? diff : max;
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("loadPages count={}, total ={}, avg ={}, min={}, max={}", count, total / 1000000.F, (total / count) / 1000000.F, min / 1000000.F, max / 1000000.F);
			LOGGER.debug("browse count={}, total ={}, avg ={}, min={}, max={}", countB, totalB / 1000000.F, (totalB / countB) / 1000000.F, minB / 1000000.F, maxB / 1000000.F);
		}
	}

	private void loadNextPage(int[] indexInOriginal, RowFile rf, int ipage, int columnIndex, ColumnInput col) {
		final long start = System.currentTimeMillis();
		try {
			col.newPage();
		} catch (RuntimeException e) {
			throw new RuntimeException("Issue on bucket=" + this.directory + " page=" + ipage + " on file=" + rf.getPost() + " on store="
					+ store.getName() + " at column " + columnIndex + "/"
					+ Arrays.toString(indexInOriginal), e);
		} catch (IOException e) {
			throw new RuntimeException("Issue on bucket=" + this.directory + " page=" + ipage + " on file=" + rf.getPost() + " on store="
					+ store.getName() + " at column " + columnIndex + "/"
					+ Arrays.toString(indexInOriginal), e);
		} finally {
			long newPageTime = System.currentTimeMillis() - start;
			if (newPageTime < FAST_LOAD_TIME) {
				if (LOGGER.isTraceEnabled()) {
					// Do not log all .newPage as one query may this many times
					LOGGER.trace(
							"Done loading bucket=" + this.directory + " page={} on file={} on index={}  at column={} in {} ms",
							ipage,
							rf.getPost(),
							store.getName(),
							columnIndex,
							newPageTime);
				}
			} else if (newPageTime < LOAD_TIME) {
				if (LOGGER.isDebugEnabled()) {
					// Do not log all .newPage as one query may this many times
					LOGGER.debug(
							"Done loading bucket=" + this.directory + " page={} on file={} on index={} at column={} in {} ms",
							ipage,
							rf.getPost(),
							store.getName(),
							columnIndex,
							newPageTime);
				}
			} else {
				// We consider a page taking more than 5 seconds is an error
				LOGGER.warn(
						"Done loading bucket=" + this.directory + " page={} on file={} on index={}  at column={} in {} ms",
						ipage,
						rf.getPost(),
						store.getName(),
						columnIndex,
						newPageTime);
			}
		}
	}

	protected int[] computerIndexOfSorted(int[] columnIds) {
		// We need to sort the column to be read. We always read the column index=0, and the next columns are appended in 'in'
		return IntStream
				.concat(IntStream.of(0), IntStream.of(columnIds).sorted().map(i -> 1 + Ints.indexOf(columnIds, i)))
				.toArray();
	}

	private boolean isDeleted(RowFile rf, int irowNum, int rowId) {
		long rowNum = rf.getStartCount() + irowNum;
		if (rowNum >= countDelete) {
			return false;
		}
		if (deleteRowNums != null && deleteRowNums.contains(rowNum)) {
			return true;
		}
		return deleteRowIds != null && deleteRowIds.contains(rowId);
	}

	private void load(RowFile rf, BucketLoader loader) throws IOException {
		int columns = store.getNumberOfColumns();
		try (MultiInputStream in = openReadCol(rf)) {
			byte[][] buf = new byte[columns][];
			int[] lg = new int[columns];
			for (int n = 0; n < columns; n++) {
				buf[n] = new byte[(store.getColumn(n).getSize() == 0) ? Short.MAX_VALUE + 2 : 8];
			}
			int rowNum = 0;
			for (int ipage = 0; ipage < rf.getPosCount().getSize(); ipage++) {
				newPageOnAllColumns(in);
				for (int irow = 0; irow < rf.getPosCount().getInt(ipage); irow++, rowNum++) {
					int rowId = in.getColumn(0).readInt();
					for (int n = 0; n < columns; n++) {
						lg[n] = in.getColumn(1 + n).readRow(buf[n], store.getColumn(n).getSize());
					}
					if (isDeleted(rf, rowNum, rowId)) {
						continue;
					}
					loader.load(rowNum, rowId, buf, lg);
				}
			}
		}
	}

	private void newPageOnAllColumns(MultiInputStream in) {
		in.forEach(c -> {
			try {
				c.newPage();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
	}

	void load(BucketLoader loader) throws IOException {
		for (RowFile rf : rowFiles) {
			load(rf, loader);
		}
	}

	void loadOne(RowFile rf, int icol, BucketLoader loader) throws IOException {
		MultiInputStream in = openReadCol(rf, new int[]{icol});
		try {
			byte[] buf = new byte[(store.getColumn(icol).getSize() == 0) ? Short.MAX_VALUE + 2 : 8];
			int rowNum = 0;
			for (int ipage = 0; ipage < rf.getPosCount().getSize(); ipage++) {
				newPageOnAllColumns(in);
				for (int irow = 0; irow < rf.getPosCount().getInt(ipage); irow++, rowNum++) {
					int rowId = in.getColumn(0).readInt();
					int lg = in.getColumn(1).readRow(buf, store.getColumn(icol).getSize());
					if (isDeleted(rf, rowNum, rowId)) {
						continue;
					}
					loader.loadOne(rowNum, rowId, buf, lg);
				}
			}
		} finally {
			IO.close(in);
		}
	}

	void loadOne(int icol, BucketLoader loader) throws IOException {
		for (RowFile rf : rowFiles) {
			loadOne(rf, icol, loader);
		}
	}

	@Override
	protected void compact() throws IOException {
		if (deleteRowNums == null && deleteRowIds == null) {
			return;
		}
		long time = System.currentTimeMillis();
		final PageBucket buc = new PageBucket(store);
		buc.init(directory, path + "c");
		buc.initOut();
		mergeTo(buc);
		buc.close();
		drop();
		init(buc);
		deleteRowNums = deleteRowIds = null;
		LOGGER.info("Compacting bucket " + path + " in " + (System.currentTimeMillis() - time) + " ms.");
	}

	private void mergeTo(final PageBucket buc) throws IOException {
		load(new BucketLoader() {
			@Override
			public void load(int rowNum, int rowId, byte[][] buf, int[] size) throws IOException {
				buc.out[0].writeInt(rowId);
				for (int n = 0; n < store.getNumberOfColumns(); n++) {
					buc.out[n + 1].writeRow(buf[n], size[n]);
				}
				buc.addRow();
			}
		});
	}

	@Override
	protected void close() throws IOException {
		if (out != null) {
			// We move to nextPage in order to flush current page
			newPage();
			// Then close each open file
			for (ColumnOutput co : out) {
				if (co != null) {
					co.close();
				}
			}
			out = null;
			modified = true;
		}
		initCommitValues();
	}

	@Override
	protected void initCommitValues() throws IOException {
		super.initCommitValues();
		rowFilesCommit = rowFiles.size();
	}

	@Override
	public void rollback() throws IOException {
		LOGGER.info("Rollbacking '" + path + "' ...");
		count = countCommit;
		size = sizeCommit;
		if (out != null) {
			for (ColumnOutput co : out) {
				if (co != null) {
					co.close();
				}
			}
			store.getDevice().delete(getColPath(ID + rowFile.getPost()));
			for (int n = 0; n < store.getNumberOfColumns(); n++) {
				store.getDevice().delete(getColPath(COL + n + rowFile.getPost()));
			}
			rowFiles.remove(rowFiles.size() - 1);
			rowFilesCommit = rowFiles.size();
			out = null;
		}
		deleteRowNums = deleteRowIds = null;
	}

	@Override
	public void save(DataOutputStream indexOut) throws IOException {
		super.save(indexOut);
		IO.save(indexOut, colFormat, colFormat.length);
		indexOut.writeInt(rowFiles.size());
		for (RowFile sav : rowFiles) {
			sav.save(indexOut);
		}
	}

	@Override
	public Bucket load(String absolutePrefix, DataInputStream stream) throws IOException {
		super.load(absolutePrefix, stream);
		colFormat = IO.load(stream, colFormat);
		int nbFiles = stream.readInt();
		for (int n = 0; n < nbFiles; n++) {
			rowFiles.add(new RowFile(store.getNumberOfColumns()).load(stream));
		}
		rowFilesCommit = rowFiles.size();
		return this;
	}
}
