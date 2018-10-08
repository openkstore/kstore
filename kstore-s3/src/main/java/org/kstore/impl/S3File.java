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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.Configuration;

/**
 *
 * @author eric
 */
public class S3File {

	private static final Logger LOGGER = LogManager.getLogger(S3File.class);

	private static final int RETRY = 3; // our own retry to be more 'AWS' independent...

	// S3 supports up to 5 gigas with put, we limit at 4 before using transfer
	static final long MAX_SIZE = 4 * 1024 * 1024 * 1024; // 4 GB
	/** Configuration. */
	private static final String KEY_ACCESS = "s3.access.key";
	private static final String KEY_SECRET = "s3.secret.key";
	/** S3 client. */
	private static AmazonS3 CLIENT;
	/** S3 transfer manager. */
	private static TransferManager transfers;

	/** S3 bucket. */
	private final String bucket;
	/** S3 key in bucket. */
	private final String key;
	/** Temporary local file. */
	private File local;

	// total count of opened streams
	private static final AtomicLong OPENED_STREAMS = new AtomicLong();
	// total count of closed streams
	private static final AtomicLong CLOSED_STREAMS = new AtomicLong();
	// the maximum number of opened streams at a givent time
	private static final AtomicLong MAX_CONCURRENT_STREAMS = new AtomicLong();

	public S3File(String fullPath) {
		int indexOfSemiColumn = fullPath.indexOf(':');
		String path;
		if (indexOfSemiColumn < 0) {
			// Typically: '/bucket/doc/file'
			if (fullPath.charAt(0) == '/') {
				path = fullPath.substring(1);
			} else {
				path = fullPath;
			}
		} else {
			// Typically: '//s3a://bucket/doc/file'
			assert fullPath.charAt(indexOfSemiColumn) == ':';
			assert fullPath.charAt(indexOfSemiColumn + 1) == '/';
			assert fullPath.charAt(indexOfSemiColumn + 2) == '/';
			path = fullPath.substring(indexOfSemiColumn + 3);
		}
		int ibucket = path.indexOf('/');
		bucket = path.substring(0, ibucket);
		key = path.substring(ibucket + 1);
	}

	public String getBucket() {
		return bucket;
	}

	public static void initClient() {
		if (CLIENT != null) {
			return;
		}
		Configuration cfg = Configuration.getInstance();
		Properties props = cfg.getProperties();
		String s3AccessKey = props.getProperty(KEY_ACCESS);
		String s3SecretKey = props.getProperty(KEY_SECRET);
		if (s3AccessKey != null) {
			System.setProperty("AWS_ACCESS_KEY_ID", s3AccessKey);
			System.setProperty("AWS_SECRET_KEY", s3SecretKey);
		}
		ClientConfiguration conf = new ClientConfiguration();
		conf.setMaxConnections(4096);
		conf.setProtocol(Protocol.HTTP);
		CLIENT = new AmazonS3Client(conf);
		transfers = new TransferManager(CLIENT);
	}

	public static void setAmazonClient(AmazonS3 s3) {
		CLIENT = s3;
		transfers = new TransferManager(CLIENT);
	}

	private InputStream open(int retry) {
		try {
			return new AwsInputStream(CLIENT.getObject(bucket, key).getObjectContent());
		} catch (SdkClientException exc) {
			if (retry - 1 == 0) {
				// dump stats informations when we really cannot read things...
				showStats();
				// we give up...
				throw exc;
			}
			return open(retry - 1);
		}
	}

	public InputStream open() throws IOException {
		return open(RETRY);
	}

	public OutputStream create() throws IOException {
		local = File.createTempFile("aws", ".tmp");
		return new AwsOutputStream(this, local, false);
	}

	public OutputStream append() throws IOException {
		local = File.createTempFile("aws", ".tmp");
		ObjectListing objectListing = CLIENT.listObjects(bucket, key);
		if (!objectListing.getObjectSummaries().isEmpty()) {
			try (InputStream is = CLIENT.getObject(bucket, key).getObjectContent()) {
				FileUtils.copyInputStreamToFile(is, local);
			}
		}
		return new AwsOutputStream(this, local, true);
	}

	public void delete() throws IOException {
		CLIENT.deleteObject(bucket, key);
	}

	public void rename(S3File dstKey) throws IOException {
		CopyObjectRequest request = new CopyObjectRequest(this.bucket, key, this.bucket, dstKey.key);
		Copy copy = transfers.copy(request);
		try {
			copy.waitForCopyResult();
			delete();
		} catch (AmazonClientException | InterruptedException ex) {
			throw new IOException("Rename fail", ex);
		}
	}

	public void save() throws IOException {
		try {
			if (local.length() < MAX_SIZE) {
				CLIENT.putObject(bucket, key, local);
				return;
			}
			// TransferManager processes all transfers asynchronously,
			// so this call returns immediately.
			Upload upload = transfers.upload(bucket, key, local);
			try {
				// wait for the upload to finish before continuing.
				upload.waitForCompletion();
			} catch (AmazonClientException | InterruptedException ex) {
				throw new IOException("Save did not complete.", ex);
			}
		} finally {
			local.delete();
		}
	}

	private static void showStats() {
		LOGGER.info("S3 streams OPENED=" + OPENED_STREAMS.get()
				+ ", CLOSED="
				+ CLOSED_STREAMS.get()
				+ ", MAX CONCURRENT="
				+ MAX_CONCURRENT_STREAMS.get()
				+ ", LEAK="
				+ (OPENED_STREAMS.get() - CLOSED_STREAMS.get()));
	}

	private static class AwsOutputStream extends BufferedOutputStream {

		private boolean closed = false;
		private final S3File s3File;

		AwsOutputStream(S3File s3File, File f, boolean append) throws FileNotFoundException {
			super(new FileOutputStream(f, append));
			this.s3File = s3File;
		}

		@Override
		public void close() throws IOException {
			if (closed) {
				return;
			}
			closed = true;
			super.close();
			s3File.save();
		}
	}

	// a wrapped InputStream to count open/close
	private class AwsInputStream extends InputStream {

		// the wrapped stream
		private final InputStream wrapped;

		public AwsInputStream(InputStream wrapped) {
			this.wrapped = wrapped;
			long opened = OPENED_STREAMS.getAndIncrement();
			// not completly threadsafe, but this a debug information, and precision is not so important...
			long concurrent = opened - CLOSED_STREAMS.get();
			MAX_CONCURRENT_STREAMS.updateAndGet(x -> (x > concurrent) ? x : concurrent);
		}

		@Override
		public boolean markSupported() {
			return wrapped.markSupported();
		}

		@Override
		public synchronized void reset() throws IOException {
			wrapped.reset();
		}

		@Override
		public synchronized void mark(int readlimit) {
			wrapped.mark(readlimit);
		}

		@Override
		public int available() throws IOException {
			return wrapped.available();
		}

		@Override
		public long skip(long n) throws IOException {
			return wrapped.skip(n);
		}

		@Override
		public int read() throws IOException {
			return wrapped.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return wrapped.read(b, off, len);
		}

		@Override
		public int read(byte[] b) throws IOException {
			return wrapped.read(b);
		}

		@Override
		public void close() throws IOException {
			wrapped.close();
			CLOSED_STREAMS.getAndIncrement();
		}
	}
}
