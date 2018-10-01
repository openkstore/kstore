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

import org.kstore.columns.ColumnInput;
import java.io.Closeable;
import java.util.function.Consumer;

/**
 * Manages multiple InputStream at the same time, typically used to enable batch operations. The underlying InputStream
 * might be actually independent InputStream, or possibly faked over a single main InputStream (e.g. when multiple
 * columns are packed in a single file)
 *
 */
public interface MultiInputStream extends Closeable {

	/**
	 *
	 * @param columnIndex
	 * @return
	 */
	ColumnInput getColumn(int columnIndex);

	/**
	 *
	 * @return the number of InputStream managed by this
	 */
	int getColumnCount();

	/**
	 *
	 * @param consumer
	 */
	void forEach(Consumer<ColumnInput> consumer);
}
