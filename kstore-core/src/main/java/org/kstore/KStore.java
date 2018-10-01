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

import java.util.List;

/**
 *
 */
public interface KStore {

	/**
	 * Get the name of this store.
	 *
	 * @return
	 */
	String getName();

	/**
	 * Gets the device.
	 *
	 * @return
	 */
	Device getDevice();

	/**
	 * Checks if this store is storing one column per file or not.
	 *
	 * @return
	 */
	boolean useOneFilePerColumn();

	/**
	 * Get the number of columns to store.
	 *
	 * @return
	 */
	int getNumberOfColumns();

	/**
	 * Get the column at the given index.
	 *
	 * @param idx
	 * @return
	 */
	Column getColumn(int idx);

	/**
	 * Get the columns list.
	 *
	 * @return
	 */
	List<Column> getColumns();

	/**
	 * Create a new bucket.
	 *
	 * @return
	 */
	Bucket newBucket();

	/**
	 * Get the list of buckets.
	 *
	 * @return
	 */
	List<Bucket> getBuckets();
}
