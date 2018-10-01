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
package org.kstore.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.Configuration;

/**
 * File manipulation can be slow, especially regarding opening file or simple getting file metadata (e.g. Amazon S3).
 * This class provides a shared Executor enabling concurrent access to InputStream resources with the proper level of
 * concurrency
 *
 */
public class BucketIOSharedPool {

	/** Logger. */
	private static final Logger LOGGER = LogManager.getLogger(BucketIOSharedPool.class);
	// We share through all cores of current node this ExecutorService: We may have 128 cores on a single machine, but a
	// pool of 16 cores to prevent opening too many files at the same time
	private static final AtomicReference<ListeningExecutorService> SHARED_EXECUTORSERVICE = new AtomicReference<>();
	private static boolean INPUTSTREAM_OPS_ARE_SEQUENTIAL = true;

	private static final AtomicLong NB_FILE_REQUESTING = new AtomicLong();
	private static final AtomicLong NB_FILE_REQUESTED = new AtomicLong();

	/**
	 * THis will force the initialization of the pool, shutting down the previous pool (if it exists) and relying on the
	 * new {@link Conf}
	 */
	@VisibleForTesting
	public static void resetInputStreamPool() {
		int nbMaxThread = Configuration.getBucketPoolSize();

		if (nbMaxThread == 0) {
			INPUTSTREAM_OPS_ARE_SEQUENTIAL = true;
			closePool(SHARED_EXECUTORSERVICE.get());
		} else {
			INPUTSTREAM_OPS_ARE_SEQUENTIAL = false;

			BasicThreadFactory threadFactory
					= new BasicThreadFactory.Builder().namingPattern("InputStreamOpener-%d").build();

			// Opening Threads can be a bit slow: we keep the N threads open for 1 minute
			ThreadPoolExecutor tpExecutor = new ThreadPoolExecutor(nbMaxThread,
					nbMaxThread,
					60,
					TimeUnit.SECONDS,
					new LinkedBlockingQueue<>(1024),
					threadFactory,
					// ThreadPoolExecutor.CallerRunsPolicy does not throw if the task is rejected
					(r, e) -> {
						if (e.isShutdown()) {
							throw new IllegalStateException("The pool is shutdown: " + e);
						} else {
							r.run();
						}
					});

			// Allow core threads to shutdown when there is no task to process (even
			// if coreSize==0, just in case we change coreSize later)
			tpExecutor.allowCoreThreadTimeOut(true);

			// Close previous pool and set this new pool as the one to use
			closePool(SHARED_EXECUTORSERVICE.getAndSet(MoreExecutors.listeningDecorator(tpExecutor)));
		}
	}

	/**
	 *
	 * @param <T>
	 * @param callable
	 * @return
	 */
	public static <T> ListenableFuture<T> submit(Callable<T> callable) {
		initPoolIfNecessary();
		if (INPUTSTREAM_OPS_ARE_SEQUENTIAL) {
			try {
				return Futures.immediateFuture(callable.call());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			long before = NB_FILE_REQUESTING.incrementAndGet();
			if (before == 0) {
				LOGGER.debug("Starting an InputStream opening session");
			} else {
				LOGGER.debug("Submitted one. " + before + " total active");
			}

			ListeningExecutorService executor = SHARED_EXECUTORSERVICE.get();
			ListenableFuture<T> future = executor.submit(callable);
			future.addListener(() -> {
				NB_FILE_REQUESTED.incrementAndGet();
				long requesting = NB_FILE_REQUESTING.decrementAndGet();

				if (requesting == 0) {
					LOGGER.debug("Completed an InputStream opening session");
				} else {
					LOGGER.debug("Completed one. " + requesting + " left active");
				}
			}, MoreExecutors.sameThreadExecutor());

			return future;
		}
	}

	// Initialize the pool if not already initialized
	// synchronized to prevent race-conditions on SHARED_EXECUTORSERVICE==null
	private synchronized static void initPoolIfNecessary() {
		if (SHARED_EXECUTORSERVICE.get() == null) {
			resetInputStreamPool();
		}
	}

	private static void closePool(ListeningExecutorService poolToClose) {
		if (poolToClose != null) {
			poolToClose.shutdown();
		}
	}
}
