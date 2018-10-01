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
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * KStore configuration.
 */
public class Configuration {

	/** Singleton pattern. */
	private static class ConfigurationHolder {
		
		private static final Configuration INSTANCE = new Configuration();
	}

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(Configuration.class);

	/** The properties. */
	private Properties properties;

	/** Default column compression type. */
	private static final String KEY_BUCKET_COMPRESSION = "bucket.compression";
	private static final Compression DEFAULT_BUCKET_COMPRESSION = Compression.SNAPPY;
	private static Compression compressionType = DEFAULT_BUCKET_COMPRESSION;

	/** Bucket page size. */
	private static final String KEY_BUCKET_PAGESIZE = "bucket.pagesize";
	private static final int DEFAULT_BUCKET_PAGESIZE = 1024;
	private static int nbPages = DEFAULT_BUCKET_PAGESIZE;

	/** Thread pool size for buckets input streams opening. */
	private static final String KEY_BUCKET_POOLSIZE = "bucket.poolsize";
	private static final int DEFAULT_BUCKET_POOLSIZE = 128;
	private static int bucketPoolSize = DEFAULT_BUCKET_POOLSIZE;
	
	private static final String KEY_BUCKET_ONEFILE = "bucket.oneFilePerColumn";
	private static final boolean DEFAULT_BUCKET_ONEFILE = true;
	private static boolean oneFilePerColumn = DEFAULT_BUCKET_ONEFILE;

	/**
	 * Hidden constructor.
	 */
	private Configuration() {
		properties = new Properties();
		try (InputStream is = Configuration.class.getClassLoader().getResourceAsStream("kstore.conf")) {
			if (is != null) {
				properties.load(is);
			} else {
				LOGGER.warn("Unable to find configuration file, will rely on default values.");
			}
		} catch (IOException e) {
			LOGGER.warn("Unable to load configuration file, will rely on default values.", e);
		}
		compressionType = Compression.valueOf(properties.getProperty(KEY_BUCKET_COMPRESSION, DEFAULT_BUCKET_COMPRESSION.name()));
		nbPages = Integer.parseInt(properties.getProperty(KEY_BUCKET_PAGESIZE, Integer.toString(DEFAULT_BUCKET_PAGESIZE)));
		bucketPoolSize = Integer.parseInt(properties.getProperty(KEY_BUCKET_POOLSIZE, Integer.toString(DEFAULT_BUCKET_POOLSIZE)));
		oneFilePerColumn = Boolean.parseBoolean(properties.getProperty(KEY_BUCKET_ONEFILE, Boolean.toString(DEFAULT_BUCKET_ONEFILE)));
	}
	
	public static Configuration getInstance() {
		return ConfigurationHolder.INSTANCE;
	}
	
	public static Compression getCompressionType() {
		return compressionType;
	}
	
	public static int getNbPages() {
		return nbPages;
	}
	
	public static int getBucketPoolSize() {
		return bucketPoolSize;
	}
	
	public Properties getProperties() {
		return properties;
	}
	
	public static boolean isOneFilePerColumn() {
		return oneFilePerColumn;
	}
	
}
