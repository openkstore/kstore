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

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.services.s3.AmazonS3;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.kstore.impl.S3Device;
import org.kstore.impl.S3File;

/**
 *
 * @author eric
 */
public class S3Test {

	private static final Logger LOGGER = LogManager.getLogger(S3Test.class);

	@ClassRule
	public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

	private static final String BUCKET_NAME = "kstore-test";
	private static final String FILE_NAME = "warehouse/test.txt";
	private static final String FILE_NAME_COPY = "warehouse/test_copy.txt";

	private static AmazonS3 s3Client;

	@BeforeClass
	public static void setUp() {
		s3Client = S3_MOCK_RULE.createS3Client();
		S3File.setAmazonClient(s3Client);
		s3Client.createBucket(BUCKET_NAME);
	}

	/**
	 *
	 */
	@Test
	public void testReadWrite() {

		final String msg = "Written in S3 file.";

		String testFile = "s3a://" + BUCKET_NAME + "/" + FILE_NAME;

		S3Device device = new S3Device();

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

		final String msg = "Written in S3 file.";

		String testFile = "s3a://" + BUCKET_NAME + "/" + FILE_NAME;

		S3Device device = new S3Device();

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

	@Test
	public void testRename() throws IOException {

		final String msg = "Written in S3 file.";

		String testFile = "s3a://" + BUCKET_NAME + "/" + FILE_NAME;
		String testCopyFile = "s3a://" + BUCKET_NAME + "/" + FILE_NAME_COPY;

		S3Device device = new S3Device();

		try (DataOutputStream out = device.create(testFile, Compression.SNAPPY, false)) {
			out.writeUTF(msg);
		} catch (IOException exc) {
			LOGGER.error("Write fail.", exc);
		}

		device.rename(testFile, testCopyFile);

		try (DataInputStream in = device.open(testCopyFile, Compression.SNAPPY)) {
			String read = in.readUTF();
			Assert.assertEquals("msg", msg, read);
		} catch (IOException exc) {
			LOGGER.error("Read fail.", exc);
		}
	}
}
