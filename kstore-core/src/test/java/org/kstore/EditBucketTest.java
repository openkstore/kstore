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
public class EditBucketTest extends StoreTest {

	private final static Object[][] DATA = {
		{"Europe", "France", 67795000L, 100.8D},
		{"Europe", "Italy", 60589445L, 201.D},
		{"Oceania", "New Zealand", 4725487L, 18.D},
		{"America", "USA", 324811000L, 33.D},
		{"America", "Colombia", 49100000L, 43.D},
		{"Asia", "Japan", 126451398L, 334.6D}
	};

	@Test
	public void testEdit() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {
			for (int i = 0; i < DATA.length; i++) {
				bucket.add(i, DATA[i]);
			}
			bucket.commit();

			bucket.deleteRowId(2);
			bucket.deleteRowId(5);

			bucket.commit();

			List<Bucket> buckets = kstore.getBuckets();

			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			List<Object[]> dataList = new ArrayList<>(Arrays.asList(DATA));
			dataList.set(2, null);
			dataList.set(5, null);
			Dumper dumper = new Dumper(dataList);

			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			Assert.assertEquals(DATA.length - 2, dumper.getCount());

		} catch (IOException exc) {
			Assert.fail("IOException during edit");
		}
	}

	@Test
	public void testRollback() {

		createBasicStore();

		Bucket bucket = kstore.newBucket();
		try {
			for (int i = 0; i < DATA.length; i++) {
				bucket.add(i, DATA[i]);
			}
			bucket.commit();

			bucket.deleteRowId(2);
			bucket.deleteRowId(5);

			bucket.rollback();

			List<Bucket> buckets = kstore.getBuckets();

			final RoaringBitmap bitRowIds = new RoaringBitmap();
			bitRowIds.add(0, 100);
			// we read all columns
			int[] columns = new int[]{0, 1, 2, 3};
			DefaultLine line = new DefaultLine(columns);

			// data list to check.
			List<Object[]> dataList = new ArrayList<>(Arrays.asList(DATA));
			Dumper dumper = new Dumper(dataList);

			for (final Bucket b : buckets) {
				b.readLines(line, bitRowIds, (int rowId, Line l) -> dumper.dumpRow(rowId, l));
			}
			Assert.assertTrue(dumper.checkAllRead());

		} catch (IOException exc) {
			Assert.fail("IOException during edit");
		}
	}

}
