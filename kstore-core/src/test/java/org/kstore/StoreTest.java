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

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.kstore.impl.DefaultColumn;
import org.kstore.impl.DefaultKStore;
import org.kstore.impl.DefaultLine;
import org.kstore.impl.FileSystemDevice;

/**
 *
 * @author eric
 */
public abstract class StoreTest {

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(StoreTest.class);
	/** The test store. */
	protected DefaultKStore kstore;

	protected void createBasicStore() {
		List<Column> schema = new ArrayList<>();
		DefaultColumn continent = new DefaultColumn(ColumnType.STRING);
		DefaultColumn country = new DefaultColumn(ColumnType.STRING);
		DefaultColumn population = new DefaultColumn(ColumnType.BIGINT);
		DefaultColumn density = new DefaultColumn(ColumnType.DOUBLE);

		schema.add(continent);
		schema.add(country);
		schema.add(population);
		schema.add(density);

		FileSystemDevice fs = new FileSystemDevice();
		kstore = new DefaultKStore("TheWorld", schema, fs);
		kstore.setDirectory("./target/buckets/");
	}

	/**
	 * A utility class to check reads.
	 */
	static class Dumper {

		/** References we want to check. */
		private final List<Object[]> references;
		/** Number of checked. */
		private int count = 0;

		public Dumper(List<Object[]> references) {
			this.references = references;
		}

		public boolean dumpRow(int rowId, Line l) {
			DefaultLine line = (DefaultLine) l;
			Object[] read = new Object[line.getSize()];
			int i = 0;
			StringBuilder sb = new StringBuilder();
			sb.append("@").append(rowId).append(" ");
			for (Object v : line.getValues()) {
				sb.append(v).append(" ");
				read[i] = v;
				i++;
			}
			LOGGER.info(sb.toString());
			// 
			Assert.assertArrayEquals("Mismatch read on row", references.get(rowId), read);
			// be sure we have all the expected lines.
			count++;
			//read until end
			return true;
		}

		public boolean checkAllRead() {
			return count == references.size();
		}

		public int getCount() {
			return count;
		}

	}
}
