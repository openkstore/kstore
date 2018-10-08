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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.kstore.columns.ColumnInput;
import org.kstore.utils.BucketIOSharedPool;
import org.kstore.utils.IO;
import org.kstore.columns.io.MultiInputStream;

/**
 * Default implementation for IMultiInputStream
 *
 */
public class MultipleInputStream implements MultiInputStream {

	protected final boolean inputStreamsAreIndependant;
	protected final ColumnInput[] inputStreams;

	public MultipleInputStream(boolean inputStreamsAreIndependant, ColumnInput[] inputStreams) {
		this.inputStreamsAreIndependant = inputStreamsAreIndependant;
		this.inputStreams = inputStreams;
	}

	@Override
	public void close() throws IOException {
		if (inputStreamsAreIndependant) {
			// Submit closing operations
			List<ListenableFuture<?>> futureInputs = Stream.of(inputStreams).map(col -> {
				return BucketIOSharedPool.submit(() -> {
					IO.close(col);

					return col;
				});
			}).collect(Collectors.toList());
			// Close streams concurrently
			Futures.getUnchecked(Futures.successfulAsList(futureInputs));
		} else {
			// Close InputStreams one after the others
			IO.close(inputStreams);
		}
	}

	@Override
	public ColumnInput getColumn(int columnIndex) {
		return inputStreams[columnIndex];
	}

	@Override
	public int getColumnCount() {
		return inputStreams.length;
	}

	@Override
	public void forEach(Consumer<ColumnInput> consumer) {
		Stream.of(inputStreams).forEach(consumer);
	}

}
