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

import org.kstore.Device;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.kstore.Compression;
import org.kstore.Configuration;

/**
 *
 */
public class HDFSDevice implements Device {

	private static final String HDFS_REPLICATION = "hdfs.replication";
	private static final short DEFAULT_HDFS_REPLICATION = -1;
	private short replication = DEFAULT_HDFS_REPLICATION;

	/**
	 *
	 */
	public HDFSDevice() {
		Configuration cfg = Configuration.getInstance();
		Properties props = cfg.getProperties();
		replication = Short.parseShort(props.getProperty(HDFS_REPLICATION, Short.toString(DEFAULT_HDFS_REPLICATION)));
	}

	public short getReplication() {
		return replication;
	}

	private InputStream openCompress(InputStream in, Compression comp) throws IOException {
		switch (comp) {
			case GZIP:
				return new GZIPInputStream(in);
			case SNAPPY:
				return new SnappyInputStream(in, false);
			default:
				return in;
		}
	}

	private OutputStream openCompress(OutputStream out, Compression comp) throws IOException {
		switch (comp) {
			case GZIP:
				return new GZIPOutputStream(out);
			case SNAPPY:
				return new SnappyOutputStream(out);
			default:
				return out;
		}
	}

	@Override
	public InputStream getInputStream(String path) throws IOException {
		return HadoopFile.get(this, path).open(path);
	}

	@Override
	public OutputStream getOutputStream(String path, boolean append) throws IOException {
		HadoopFile hdfs = HadoopFile.get(this, path);
		hdfs.mkdirs(path.substring(0, path.lastIndexOf('/')));
		return (append) ? hdfs.append(path) : hdfs.create(path);
	}

	@Override
	public DataInputStream open(String path, Compression comp) throws IOException {
		return new DataInputStream(new BufferedInputStream(openCompress(getInputStream(path), comp)));
	}

	@Override
	public DataOutputStream create(String path, Compression comp, boolean append) throws IOException {
		OutputStream out = new BufferedOutputStream(getOutputStream(path, append));
		return new DataOutputStream(openCompress(out, comp));
	}

	@Override
	public void delete(String path) throws IOException {
		HadoopFile.get(this, path).delete(path);
	}

	@Override
	public void rename(String src, String dst) throws IOException {
		HadoopFile.get(this, src).rename(src, dst);
	}
}
