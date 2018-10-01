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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.Bucket;
import org.kstore.Column;
import org.kstore.ColumnType;
import org.kstore.Line;
import org.kstore.impl.DefaultColumn;
import org.kstore.impl.DefaultKStore;
import org.kstore.impl.DefaultLine;
import org.kstore.impl.FileSystemDevice;
import org.roaringbitmap.RoaringBitmap;

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
			DefaultColumn continent = new DefaultColumn(ColumnType.STRING);
			DefaultColumn country = new DefaultColumn(ColumnType.STRING);
			DefaultColumn population = new DefaultColumn(ColumnType.BIGINT);
			DefaultColumn density = new DefaultColumn(ColumnType.DOUBLE);

			schema.add(continent);
			schema.add(country);
			schema.add(population);
			schema.add(density);

			// create our KStore on a file system device
			FileSystemDevice fs = new FileSystemDevice();
			DefaultKStore kstore = new DefaultKStore("TheWorld", schema, fs);
			Path temp = Files.createTempDirectory("kstore");
			kstore.setDirectory(temp.toString());

			// create a bucket to enter some data
			Bucket bucket = kstore.newBucket();
			// insert lines in out bucket
			bucket.add(0, new Object[]{"Europe", "France", 67795000L, 100.8D});
			bucket.add(1, new Object[]{"Oceania", "New Zealand", 4725487L, 18.D});
			bucket.add(2, new Object[]{"Asia", "Japan", 126451398L, 334.6D});
			// commit our changes
			bucket.commit();

			// let's read back our buckets
			List<Bucket> buckets = kstore.getBuckets();

			// selects the lines and columns we are interested in
			final RoaringBitmap bitRowIds = new RoaringBitmap();
			// we want to read all lines
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};

			// our reading vector
			DefaultLine line = new DefaultLine(columns.length);

			// read !
			for (final Bucket b : buckets) {
				b.readLines(line, columns, bitRowIds, (int rowId, Line l) -> showMeTheLine(rowId, l));
			}
			// save our store
			kstore.save ();
			
			LOGGER.info("Checking a copy of this KStore...");
			// finally re-create a store and check 
			DefaultKStore kstoreCopy = new DefaultKStore("TheWorld", schema, fs);
			kstoreCopy.setDirectory(temp.toString());
			kstoreCopy.load();
			
			buckets = kstoreCopy.getBuckets();
			// read !
			for (final Bucket b : buckets) {
				b.readLines(line, columns, bitRowIds, (int rowId, Line l) -> showMeTheLine(rowId, l));
			}
			
		} catch (IOException ex) {
			LOGGER.error("Sample failure.", ex);
		}
		
		LOGGER.info("Good bye!");
		System.exit(0);
	}

	private static boolean showMeTheLine(int rowId, Line l) {
		DefaultLine line = (DefaultLine) l;
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
