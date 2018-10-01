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
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is used to hold multiple columns in a single file. Each column write one
 * page before moving to next row: the file looks like:
 * Column0Page0_Column1Page0_Column1Page0_Column1Page1_EOF
 *
 * HDFS does not accepting opening too many files in parallel. The KStore
 * expects to write one file per column. Then, loading a table with 128 columns
 * in a cluster of 8 nodes leads to opening 5*128=1024 files at the same time.
 * This class enables keeping data in a buffer in order to prevent opening too
 * many files at the same time.
 *
 */
public class MultipleColumnPageBytesOutput implements ColumnPageBytesOutput {

	private final AtomicInteger nbColumns = new AtomicInteger();
	private final OutputStream outputStream;

	public MultipleColumnPageBytesOutput(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	@Override
	public void writeNextPageBytes(byte[] rows) throws IOException {
		outputStream.write(rows);
	}

	@Override
	public void closeColumn() throws IOException {
		if (nbColumns.decrementAndGet() == 0) {
			// All columns are closed: we can close the single file holding all columns
			outputStream.close();
		}
	}

	public void incrementNbColumns() {
		nbColumns.incrementAndGet();
	}

}
