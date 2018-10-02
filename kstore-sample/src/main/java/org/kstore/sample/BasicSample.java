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
package org.kstore.sample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.Bucket;
import org.kstore.Column;
import org.kstore.ColumnType;
import org.kstore.KStore;
import org.kstore.Line;
import org.kstore.Range;
import org.kstore.impl.DefaultColumn;
import org.kstore.impl.DefaultKStore;
import org.kstore.impl.DefaultLine;
import org.kstore.utils.BucketIOSharedPool;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author eric
 */
public class BasicSample {

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(BasicSample.class);

	public static void main(String[] argv) {
		try {
			// let's define our schema
			List<Column> schema = new ArrayList<>();
			Column country = new DefaultColumn(ColumnType.STRING);
			Column population = new DefaultColumn(ColumnType.BIGINT);
			schema.add(country);
			schema.add(population);

			// create our KStore on a file system device
			Path temp = Files.createTempDirectory("kstore");
			KStore kstore = new DefaultKStore("TheWorld", schema, temp.toString());

			// create a bucket to enter some data
			Bucket bucket = kstore.newBucket();
			// insert lines in out bucket
			bucket.add(0, new Object[]{"France", 67795000L})
			.add(1, new Object[]{"New Zealand", 4725487L})
			.add(2, new Object[]{"Japan", 126451398L})
			.commit();

			// Select the lines we are interested in. In this case, the two first lines
			Range range = new Range(0, 2);
			// our reading vector
			Line line = new DefaultLine(0, 1);
			bucket.readLines(line, range, (int rowId, Line l) -> showMeTheLine(rowId, l));
			// Close
			kstore.close();
		} catch (IOException ex) {
			LOGGER.error("Sample failure.", ex);
		}
	}

	private static boolean showMeTheLine(int rowId, Line line) {
		StringBuilder sb = new StringBuilder();
		sb.append("@").append(rowId).append(" ");
		for (Object v : line.getValues()) {
			sb.append(v).append(" ");
		}
		LOGGER.info(sb.toString());
		//read until end
		return true;
	}
}
