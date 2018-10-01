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
package org.kstore.impl;

import org.kstore.Column;
import org.kstore.ColumnType;

/**
 *
 */
public class DefaultColumn implements Column {

	/** The column type. */
	private final ColumnType type;
	/** Column data type size. */
	private final int size;
	/** Does this column stores something or not. */
	private boolean isCalculated;

	public DefaultColumn(ColumnType type) {
		this.type = type;
		switch (type) {
			case TINYINT:
				size = 1;
				break;
			case SMALLINT:
				size = 2;
				break;
			case INT:
			case FLOAT:
				size = 4;
				break;
			case BIGINT:
			case DOUBLE:
				size = 8;
				break;
			default:
				size = 0;
				break;
		}
	}

	@Override
	public ColumnType getColumnType() {
		return type;
	}

	@Override
	public int getSize() {
		return size;
	}

	@Override
	public boolean isCalculated() {
		return isCalculated;
	}

	public void setIsCalculated(boolean isCalculated) {
		this.isCalculated = isCalculated;
	}

}
