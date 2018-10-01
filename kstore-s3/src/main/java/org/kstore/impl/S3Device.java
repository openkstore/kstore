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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.kstore.Compression;

/**
 *
 */
public class S3Device implements Device {

	/**
	 *
	 */
	public S3Device() {
		S3File.initClient();
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
		return new S3File(path).open();
	}

	@Override
	public OutputStream getOutputStream(String path, boolean append) throws IOException {
		return append ? new S3File(path).append() : new S3File(path).create();
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
		new S3File(path).delete();
	}

	@Override
	public void rename(String src, String dst) throws IOException {
		new S3File(src).rename(new S3File(dst));
	}
}
