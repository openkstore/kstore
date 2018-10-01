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
package org.kstore.columns.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.kstore.utils.ArrayInt;

/**
 *
 * @see MultipleColumnPageBytesOutput
 */
public class MultipleColumnPageBytesInput {

	private final AtomicInteger nbColumns = new AtomicInteger();
	private final InputStream inputStream;
	private final ArrayInt[] columnIndexToPageIndexes;

	private final Set<Integer> openedColumns = new TreeSet<>();

	// Initially, we are on first row and about to read the first column
	final AtomicInteger currentRow = new AtomicInteger();
	final AtomicInteger nextColumn = new AtomicInteger();

	public MultipleColumnPageBytesInput(ArrayInt[] columnIndexToPageIndexes, InputStream inputStream) {
		this.columnIndexToPageIndexes = columnIndexToPageIndexes;
		this.inputStream = inputStream;
	}

	public int readBytes(byte[] b, int off, int len) throws IOException {
		return inputStream.read(b, off, len);
	}

	public void closeColumn() throws IOException {
		if (nbColumns.decrementAndGet() == 0) {
			// All columns are closed: we can close the single file holding all columns
			inputStream.close();
		}
	}

	private void incrementNbColumns() {
		nbColumns.incrementAndGet();
	}

	/**
	 * Skip between to position the cursor on give column, moving to next row if
	 * necessary
	 *
	 * @param columnIndex
	 * the column to which we want to move
	 * @throws IOException
	 */
	protected void skipToColumn(int columnIndex) throws IOException {
		int localNextColumn = nextColumn.get();
		if (columnIndex != localNextColumn) {
			// We have some pages to skip
			long nbToSKip = 0L;
			if (columnIndex > localNextColumn) {
				int currentrow = currentRow.get();
				// We look for a next column of current row
				nbToSKip += bytesBetweenColumns(localNextColumn, columnIndex, currentrow);
			} else {
				// The next page for given column is on next row
				int nextRow = currentRow.incrementAndGet();
				int previousRow = nextRow - 1;

				// Move until end of this row
				// -1 to exclude idColumn
				nbToSKip += bytesBetweenColumns(localNextColumn, columnIndexToPageIndexes.length - 1, previousRow);

				if (previousRow < columnIndexToPageIndexes[0].getInts().length) {
					// Move to requested column: From 1 to skip idColumnPositions
					nbToSKip += bytesBetweenColumns(0, columnIndex, nextRow);
				}
			}

			inputStream.skip(nbToSKip);

			nextColumn.set(columnIndex);
		}
	}

	private long bytesBetweenColumns(int fromColumn, int toColumn, int row) {
		// +1 to skip idColumn
		return IntStream.range(fromColumn + 1, toColumn + 1).mapToLong(c -> columnIndexToPageIndexes[c].getInt(row)).sum();
	}

	// Current read moves us to next page
	private void adjustNewPosition() {
		// +1: we check if next column does exist
		// -1: we skip the idColumn
		if (nextColumn.get() + 1 == columnIndexToPageIndexes.length - 1) {
			// We are on last column: move to next row
			nextColumn.set(0);
			currentRow.incrementAndGet();
		} else {
			// Move to next column of current row
			nextColumn.incrementAndGet();
		}
	}

	public ColumnPageBytesInput popColumnReader(int columnIndex) {
		if (!openedColumns.add(columnIndex)) {
			throw new IllegalStateException("The column #" + columnIndex + " has already been open");
		}
		incrementNbColumns();

		return new ColumnPageBytesInput() {

			@Override
			public int readNextPage(byte[] b, int off, int len) throws IOException {
				MultipleColumnPageBytesInput.this.skipToColumn(columnIndex);

				// +1 to skip idColumn
				ArrayInt currentColumnPagePositions = columnIndexToPageIndexes[nextColumn.get() + 1];
				int currentPageRow = currentRow.get();
				if (currentPageRow >= currentColumnPagePositions.getInts().length) {
					// There is no next row
					return -1;
				}
				int currentPageLength = currentColumnPagePositions.getInt(currentPageRow);
				if (len < currentPageLength) {
					// TODO It may be legit to ask only the first bytes of given page. Still, next
					// call for current column should move moving to next page
					throw new IllegalArgumentException("We should request at least the next page");
				} else if (len > currentPageLength) {
					// The caller is certainly trying to fill a buffer: we will return less bytes as
					// only current page is available
					len = currentPageLength;
				}

				int totalNbRead = 0;

				// We need to fully read the page, as we will adjust the cursor position after reading bytes
				while (totalNbRead < len) {
					int nbRead = MultipleColumnPageBytesInput.this.readBytes(b, off + totalNbRead, len - totalNbRead);

					if (nbRead < 0) {
						throw new IllegalStateException("The IS has not the number of bytes for given page: " + totalNbRead + "/" + len);
					}

					totalNbRead += nbRead;
				}

				// Adjust internals due to the fact we have read a page
				MultipleColumnPageBytesInput.this.adjustNewPosition();

				return totalNbRead;
			}

			@Override
			public void closeColumn() throws IOException {
				MultipleColumnPageBytesInput.this.closeColumn();
			}
		};
	}

}
