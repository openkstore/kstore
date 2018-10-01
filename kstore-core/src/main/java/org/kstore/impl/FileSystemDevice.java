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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
public class FileSystemDevice implements Device {

	/**
	 *
	 */
	public FileSystemDevice() {
	}

	/**
	 * Opens a compress stream for reading.
	 * @param in
	 * @param comp
	 * @return
	 * @throws IOException 
	 */
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

	/**
	 * Opens a compress stream for writing.
	 * @param out
	 * @param comp
	 * @return
	 * @throws IOException 
	 */
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
	public DataInputStream open(String path, Compression comp) throws IOException {
		return new DataInputStream(new BufferedInputStream(openCompress(getInputStream(path), comp)));
	}

	@Override
	public InputStream getInputStream(String path) throws IOException {
		return new FileInputStream(path);
	}

	@Override
	public OutputStream getOutputStream(String path, boolean append) throws IOException {
		File dir = new File(path).getParentFile();
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return new FileOutputStream(path, append);
	}

	@Override
	public DataOutputStream create(String path, Compression comp, boolean append) throws IOException {
		OutputStream out = new BufferedOutputStream(getOutputStream(path, append));
		return new DataOutputStream(openCompress(out, comp));
	}

	@Override
	public void delete(String path) throws IOException {
		new File(path).delete();
	}

	@Override
	public void rename(String src, String dst) throws IOException {
		new File(src).renameTo(new File(dst));
	}
}
