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

/**
 * A column of a KStore.
 */
public interface Column {
	
	/**
	 * Gets the column type
	 * @return 
	 */
	ColumnType getColumnType ();
	
	/**
	 * Gets the size of the column.
	 * @return 
	 */
	int getSize ();
	
	/**
	 * If the column is a calculated column, values will not be stored.
	 * @return 
	 */
	boolean isCalculated ();
}
