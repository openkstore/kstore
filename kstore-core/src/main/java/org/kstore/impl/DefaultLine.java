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

import java.util.Arrays;

import org.kstore.Line;
import org.kstore.utils.Str;

/**
 *
 */
public class DefaultLine implements Line {

	/** Line of values. */
	private final Object[] values;
	/** Columns that we want to read */
	private final int[] columns;

	public DefaultLine(int... columns) {
		this.columns = columns;
		this.values = new Object[columns.length];
	}

	@Override
	public void addLong(int colId, long value) {
		values[colId] = value;
	}

	@Override
	public void addDouble(int colId, double value) {
		values[colId] = value;
	}

	@Override
	public void addString(int colId, Str value) {
		values[colId] = value.toString();
	}

	public int[] getColumns() {
		return columns;
	}

	public Object[] getValues() {
		return values;
	}

	public int getSize() {
		return values.length;
	}

	public void reset() {
		for (int i = 0; i < values.length; i++) {
			values[i] = null;
		}
	}

	@Override
	public String toString() {
		return "DefaultLine [values=" + Arrays.toString(values) + "]";
	}
}
