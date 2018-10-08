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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.kstore.impl.DefaultLine;
import org.roaringbitmap.RoaringBitmap;

/**
 *
 * @author eric
 */
public class SimpleStoreTest extends StoreTest {

	private final static Object[][] DATA = {
		{"Europe", "France", 67795000L, 100.8D},
		{"Europe", "Italy", 60589445L, 201.D},
		{"Oceania", "New Zealand", 4725487L, 18.D}
	};

	private final static Object[][] DATA_2 = {
		{"America", "USA", 324811000L, 33.D},
		{"America", "Colombia", 49100000L, 43.D},
		{"Asia", "Japan", 126451398L, 334.6D}
	};

	@Test
	public void testReadWrite() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {
			bucket.add(0, DATA[0]);
			bucket.add(1, DATA[1]);
			bucket.add(2, DATA[2]);
			bucket.commit();
			List<Bucket> buckets = kstore.getBuckets();

			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			List<Object[]> dataList = Arrays.asList(DATA);
			Dumper dumper = new Dumper(dataList);
			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			Assert.assertTrue(dumper.checkAllRead());

		} catch (IOException exc) {
			Assert.fail("IOException during read/write");
		}
	}

	@Test
	public void testMultipleWrite() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {
			bucket.add(0, DATA[0]);
			bucket.add(1, DATA[1]);
			bucket.add(2, DATA[2]);
			bucket.commit();

			bucket.add(3, DATA_2[0]);
			bucket.add(4, DATA_2[1]);
			bucket.add(5, DATA_2[2]);
			bucket.commit();

			List<Bucket> buckets = kstore.getBuckets();

			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			List<Object[]> dataList = new ArrayList<>();
			dataList.addAll(Arrays.asList(DATA));
			dataList.addAll(Arrays.asList(DATA_2));
			Dumper dumper = new Dumper(dataList);

			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			Assert.assertTrue(dumper.checkAllRead());

		} catch (IOException exc) {
			Assert.fail("IOException during read/write");
		}
	}

	@Test
	public void testSingleLineRead() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {
			bucket.add(0, DATA[0]);
			bucket.add(1, DATA[1]);
			bucket.add(2, DATA[2]);
			bucket.commit();

			bucket.add(3, DATA_2[0]);
			bucket.add(4, DATA_2[1]);
			bucket.add(5, DATA_2[2]);
			bucket.commit();

			List<Bucket> buckets = kstore.getBuckets();

			// only read line 1 and 4
			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(1); // line 1 == DATA[1]
			bitRowIds.add(4); // line 4 == DATA[1]
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			ArrayList<Object[]> dataList = new ArrayList<>(6);
			dataList.addAll(Arrays.asList(DATA));
			dataList.addAll(Arrays.asList(DATA_2));
			Dumper dumper = new Dumper(dataList);
			//
			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			// be sure we only read two lines
			Assert.assertEquals(2, dumper.getCount());

		} catch (IOException exc) {
			Assert.fail("IOException during read/write");
		}
	}

	@Test
	public void testRollback() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {

			bucket.add(3, DATA_2[0]);
			bucket.add(4, DATA_2[1]);
			bucket.add(5, DATA_2[2]);

			bucket.rollback();

			bucket.add(0, DATA[0]);
			bucket.add(1, DATA[1]);
			bucket.add(2, DATA[2]);

			bucket.commit();

			List<Bucket> buckets = kstore.getBuckets();

			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			List<Object[]> dataList = new ArrayList<>();
			dataList.addAll(Arrays.asList(DATA));
			Dumper dumper = new Dumper(dataList);

			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			Assert.assertTrue(dumper.checkAllRead());

		} catch (IOException exc) {
			Assert.fail("IOException during testRollback");
		}
	}
}
