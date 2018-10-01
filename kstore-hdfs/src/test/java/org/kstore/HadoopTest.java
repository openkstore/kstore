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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.kstore.impl.HDFSDevice;

/**
 *
 * @author eric
 */
public class HadoopTest {

	private static File baseDir;

	private static MiniDFSCluster hdfsCluster;

	private static String hdfsURI;

	private static final Logger LOGGER = LogManager.getLogger(HadoopTest.class);

	@BeforeClass
	public static void setUp() {
		try {
			baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
			DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
		} catch (IOException ex) {
			LOGGER.error("Fail test", ex);
		}
	}

	@AfterClass
	public static void tearDown() {
		hdfsCluster.shutdown();
		FileUtil.fullyDelete(baseDir);
	}

	@Test
	public void testReadWrite() {

		final String msg = "Written in HADOOP file.";

		String testFile = hdfsURI + "warehouse/test.txt";

		HDFSDevice device = new HDFSDevice();

		try (DataOutputStream out = device.create(testFile, Compression.NONE, false)) {
			out.writeUTF(msg);
		} catch (IOException exc) {
			LOGGER.error("Write fail.", exc);
		}

		try (DataInputStream in = device.open(testFile, Compression.NONE)) {
			String read = in.readUTF();
			Assert.assertEquals("msg", msg, read);
		} catch (IOException exc) {
			LOGGER.error("Read fail.", exc);
		}
	}

	@Test
	public void testReadWriteCompression() {

		final String msg = "Written in HADOOP file.";

		String testFile = hdfsURI + "warehouse/test.txt";

		HDFSDevice device = new HDFSDevice();

		try (DataOutputStream out = device.create(testFile, Compression.SNAPPY, false)) {
			out.writeUTF(msg);
		} catch (IOException exc) {
			LOGGER.error("Write fail.", exc);
		}

		try (DataInputStream in = device.open(testFile, Compression.SNAPPY)) {
			String read = in.readUTF();
			Assert.assertEquals("msg", msg, read);
		} catch (IOException exc) {
			LOGGER.error("Read fail.", exc);
		}
	}
	
	@Ignore
	@Test
	public void testAppend() {

		final String msg = "Written in HADOOP file.";

		String testFile = hdfsURI + "warehouse/test.txt";

		HDFSDevice device = new HDFSDevice();

		//first write
		try (DataOutputStream out = device.create(testFile, Compression.SNAPPY, false)) {
			out.writeUTF(msg);
		} catch (IOException exc) {
			LOGGER.error("Write fail.", exc);
		}

		try (DataOutputStream out = device.create(testFile, Compression.SNAPPY, true)) {
			out.writeUTF(msg);
		} catch (IOException exc) {
			LOGGER.error("Write fail.", exc);
		}
		
		try (DataInputStream in = device.open(testFile, Compression.SNAPPY)) {
			String read = in.readUTF();
			Assert.assertEquals("msg", msg + msg, read);
		} catch (IOException exc) {
			LOGGER.error("Read fail.", exc);
		}
	}
}
