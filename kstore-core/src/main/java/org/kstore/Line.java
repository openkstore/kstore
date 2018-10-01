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

import org.kstore.utils.Str;

/**
 * The line  allows to read one line from a bucket.
 */
public interface Line {

	/**
	 * Adds a long value at the given index column.
	 * @param colId
	 * @param value 
	 */
	void addLong (int colId, long value);
	
	/**
	 * Adds a double value at the given index column.
	 * @param colId
	 * @param value 
	 */
	void addDouble (int colId, double value);

	/**
	 * Adds a string value at the given index column.
	 * @param colId
	 * @param value 
	 */
	void addString (int colId, Str value);
}
