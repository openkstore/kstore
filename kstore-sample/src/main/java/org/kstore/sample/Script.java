package org.kstore.sample;
/*
 * @author Olivier Pitton <olivier@indexima.com>
 */

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kstore.Bucket;
import org.kstore.Column;
import org.kstore.ColumnType;
import org.kstore.KStore;
import org.kstore.Line;
import org.kstore.Range;
import org.kstore.impl.DefaultColumn;
import org.kstore.impl.DefaultKStore;
import org.kstore.impl.DefaultLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Script {

	private static final Logger LOGGER = LogManager.getLogger(Script.class);

	public static void main(String[] argv) throws IOException  {
		List<Column> schema = Lists.newArrayList(new DefaultColumn(ColumnType.STRING));

		Path temp = Files.createTempDirectory("kstore");
		KStore kStore = new DefaultKStore("My KStore", schema, temp.toString());

		Bucket bucket = kStore.newBucket();
		bucket
			.add(0, new Object[]{"France"})
			.add(1, new Object[]{"Spain"})
			.commit();

		Range range = new Range(0, 2);
		Line line = new DefaultLine(0);
		bucket.readLines(line, range, (int rowId, Line l) -> showMeTheLine(rowId, l));
		kStore.close();
	}

	private static boolean showMeTheLine(int rowId, Line line) {
		StringBuilder sb = new StringBuilder();
		sb.append("@").append(rowId).append(" ");
		for (Object v : line.getValues()) {
			sb.append(v).append(" ");
		}
		LOGGER.info(sb.toString());
		//read until end
		return true;
	}
}
