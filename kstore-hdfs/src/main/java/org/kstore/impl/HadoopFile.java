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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author eric
 */
public class HadoopFile {

	/** HADOOP file system. */
	private FileSystem fs;
	/** HADOOP configuration. */
	private final Configuration conf;
	/** Security. */
	private static UserGroupInformation ugi;
	/** File replication. */
	private short replication = 1;

	/** File cache. */
	static ConcurrentHashMap<String, HadoopFile> hfiles = new ConcurrentHashMap<String, HadoopFile>();

	private HadoopFile(String namenode, short replication) throws IOException {
		conf = new Configuration();
		setConf(namenode, conf);
		this.replication = replication;
	}

	private static void setConf(String namenode, Configuration conf) throws IOException {
		if (namenode != null) {
			conf.set("fs.defaultFS", namenode);
		}
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("yarn-site.xml");
	}

	public static HadoopFile get(HDFSDevice device, String path) throws IOException {
		String namenode = path.substring(0, path.indexOf('/', 7));
		String key = namenode + UserGroupInformation.getCurrentUser().getShortUserName();
		HadoopFile file = hfiles.get(key);
		if (file == null) {
			hfiles.put(key, file = new HadoopFile(namenode, device.getReplication()));
		}
		file.fs = FileSystem.get(file.conf);
		return file;
	}

	public static String noHdfs(String path) {
		return path.substring(path.indexOf('/', 7));
	}

	public DataInputStream open(String path) throws IOException {
		String hdfsPath = noHdfs(path);
		return fs.open(new Path(hdfsPath));
	}

	public OutputStream create(String path) throws IOException {
		String hdfsPath = noHdfs(path);
		if (replication == -1) {
			return fs.create(new Path(hdfsPath), true);
		} else {
			return fs.create(new Path(hdfsPath), replication);
		}
	}

	public OutputStream append(String path) throws IOException {
		String hdfsPath = noHdfs(path);
		Path p = new Path(hdfsPath);
		if (fs.exists(p)) {
			return fs.append(p);
		} else {
			return fs.create(p, false);
		}
	}

	public void mkdirs(String path) throws IOException {
		String hdfsPath = noHdfs(path);
		fs.mkdirs(new Path(hdfsPath));
	}

	public void rename(String src, String dst) throws IOException {
		String hdfsSrc = noHdfs(src);
		String hdfsDest = noHdfs(dst);
		fs.rename(new Path(hdfsSrc), new Path(hdfsDest));
	}

	public void delete(String path) throws IOException {
		String hdfsPath = noHdfs(path);
		fs.delete(new Path(hdfsPath), true);
	}
}
