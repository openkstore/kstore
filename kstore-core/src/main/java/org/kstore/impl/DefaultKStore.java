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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.kstore.Bucket;
import org.kstore.Column;
import org.kstore.Compression;
import org.kstore.Configuration;
import org.kstore.Device;
import org.kstore.KStore;
import org.kstore.PageBucket;

/**
 *
 */
public class DefaultKStore implements KStore {

	/** Bucket file names. */
	private final static String BUCKET_NAME = "bucket";
	/** Index name. */
	private final static String INDEX_NAME = "index";

	/** Where to store the buckets. */
	private String directory;
	/** Name of the store. */
	private final String name;
	/** Device of the store. */
	private final Device device;
	/** Columns of the store. */
	private final List<Column> columns;
	/** Buckets of the store. */
	private final List<Bucket> buckets = new ArrayList<>();

	public DefaultKStore(String name, List<Column> columns, File directory) {
		this(name, columns, directory.getAbsolutePath());
	}

	public DefaultKStore(String name, List<Column> columns, Path directory) {
		this(name, columns, directory.toString());
	}

	public DefaultKStore(String name, List<Column> columns, String directory) {
		this(name, columns, directory, new FileSystemDevice());
	}

	public DefaultKStore(String name, List<Column> columns, String directory, Device device) {
		this.name = name;
		this.columns = columns;
		setDirectory(directory);
		this.device = device;
	}

	@Override
	public Device getDevice() {
		return device;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean useOneFilePerColumn() {
		return Configuration.isOneFilePerColumn();
	}

	@Override
	public int getNumberOfColumns() {
		return columns.size();
	}

	@Override
	public Column getColumn(int idx) {
		return columns.get(idx);
	}

	@Override
	public List<Column> getColumns() {
		return Collections.unmodifiableList(columns);
	}

	@Override
	public Bucket newBucket() {
		PageBucket pb = new PageBucket(this);
		pb.init(directory, getRelativeBucketPath(0));
		buckets.add(pb);
		return pb;
	}

	@Override
	public List<Bucket> getBuckets() {
		return Collections.unmodifiableList(buckets);
	}

	public String getDirectory() {
		return directory;
	}

	private void setDirectory(String directory) {
		this.directory = directory;
		// be sure directory path is well terminated
		if (!directory.endsWith("/")) {
			this.directory += "/";
		}
	}

	private String getRelativeBucketPath(int id) {
		return BUCKET_NAME + id;
	}

	public void save() throws IOException {
		try (DataOutputStream out = device.create(directory + INDEX_NAME, Compression.NONE, false)) {
			out.writeInt(buckets.size());
			for (Bucket b : buckets) {
				b.save(out);
			}
		}
	}

	public void load() throws IOException {
		try (DataInputStream in = device.open(directory + INDEX_NAME, Compression.NONE)) {
			int nbBuckets = in.readInt();
			for (int n = 0; n < nbBuckets; n++) {
				newBucket().load(directory, in);
			}
		}
	}
}
