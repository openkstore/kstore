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

import org.junit.Assert;
import org.junit.Test;

public class S3FileTest {
	@Test
	public void testMissingProtocol_noSlashPrefix() {
		S3File file = new S3File("root/folder");
		Assert.assertEquals("root", file.getBucket());
	}

	@Test
	public void testMissingProtocol_slashPrefix() {
		S3File file = new S3File("/root/folder");
		Assert.assertEquals("root", file.getBucket());
	}

	@Test
	public void testWithProtocol() {
		S3File file = new S3File("s3a://root/folder");
		Assert.assertEquals("root", file.getBucket());
	}
}
