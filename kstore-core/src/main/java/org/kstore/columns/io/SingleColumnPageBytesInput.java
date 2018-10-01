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

/**
 * 
 */
public class SingleColumnPageBytesInput implements ColumnPageBytesInput {

	private final InputStream inputStream;

	public SingleColumnPageBytesInput(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	@Override
	public int readNextPage(byte b[], int off, int len) throws IOException {
		return inputStream.read(b, off, len);
	}

	@Override
	public void closeColumn() throws IOException {
		inputStream.close();
	}

}
