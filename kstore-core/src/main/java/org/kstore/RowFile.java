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
import org.kstore.utils.ArrayInt;
import org.kstore.utils.IO;

public class RowFile {

	private final ArrayInt[] pos;
	private ArrayInt posCount;
	private String post;
	private long startCount;

	RowFile(int nbVals) {
		pos = new ArrayInt[nbVals + 1];
	}

	public ArrayInt[] getPos() {
		return pos;
	}

	public ArrayInt getPos(int i) {
		return pos[i];
	}

	public ArrayInt getPosCount() {
		return posCount;
	}

	public String getPost() {
		return post;
	}

	public long getStartCount() {
		return startCount;
	}

	RowFile init(String post, long startCount) {
		this.post = post;
		this.startCount = startCount;
		if (posCount == null) {
			posCount = new ArrayInt().init(1024);
			for (int n = 0; n < pos.length; n++) {
				pos[n] = new ArrayInt().init(1024);
			}
		}
		return this;
	}

	void save(DataOutputStream out) throws IOException {
		IO.save(out, post);
		IO.save(out, posCount);
		for (ArrayInt p : pos) {
			IO.save(out, p);
		}
		out.writeLong(startCount);
	}

	RowFile load(DataInputStream in) throws IOException {
		post = IO.loadString(in);
		posCount = IO.load(in, posCount);
		for (int n = 0; n < pos.length; n++) {
			pos[n] = IO.load(in, pos[n]);
		}
		startCount = in.readLong();
		return this;
	}
}
