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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The device on which the store will physically write.
 */
public interface Device {

	/**
	 * Opens the given path for reading, with the given compression scheme.
	 *
	 * @param path
	 * @param comp
	 * @return
	 * @throws IOException
	 */
	DataInputStream open(String path, Compression comp) throws IOException;

	/**
	 * Creates or appends the given path with the given compression scheme and get a stream to write into.
	 *
	 * @param path
	 * @param comp
	 * @param append
	 * @return
	 * @throws IOException
	 */
	DataOutputStream create(String path, Compression comp, boolean append) throws IOException;

	/**
	 * Deletes the given path.
	 *
	 * @param path
	 * @throws IOException
	 */
	void delete(String path) throws IOException;

	/**
	 * Renames the given path.
	 *
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	void rename(String src, String dst) throws IOException;

	/**
	 * Get an input stream on the given path.
	 *
	 * @param path
	 * @return
	 * @throws IOException
	 */
	InputStream getInputStream(String path) throws IOException;

	/**
	 * Get an output stream on the given path.
	 *
	 * @param path
	 * @param append
	 * @return
	 * @throws IOException
	 */
	OutputStream getOutputStream(String path, boolean append) throws IOException;

}
