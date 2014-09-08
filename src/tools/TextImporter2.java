// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

	private static final List<String> metricTags = new ArrayList<String>();
	private static final List<TimeValue> dataPoints = new ArrayList<TimeValue>();

	/** Prints usage and exits with the given retval.  */
	static void usage(final ArgP argp, final int retval) {
		System.err.println("Usage: import path [more paths]");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(retval);
	}

	static long parseDuration(String arg) {
		if (arg == null) return 0;

		if (arg.charAt(0) == '-') {
			return -DateTime.parseDuration(arg.substring(1, arg.length()));
		} else {
			return DateTime.parseDuration(arg);
		}
	}

	static int parseRepeatCount(String arg) {
		if (arg == null) return 1; // this is an optional parameter, so it may be missing

		//TODO throw exception if count <= 0
		return Integer.parseInt(arg);
	}

	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);

		args = CliOptions.parse(argp, args);
		if (args == null) {
			usage(argp, 1);
		} else if (args.length < 1) { //TODO change this to account for our own parameters
			usage(argp, 2);
		}

		// get a config object
		Config config = CliOptions.getConfig(argp);

		final TSDB tsdb = new TSDB(config);
		tsdb.checkNecessaryTablesExist().joinUninterruptibly();
		argp = null;
		try {
			final long start_freemem = Runtime.getRuntime().freeMemory();

			int points = 0;
			final long start_time = System.nanoTime();

			for (final String path : args) {
				points += importFile(path);
			}

			final long avg_dp_size = (start_freemem - Runtime.getRuntime().freeMemory()) / points;
			LOG.info("Average datapoint size = {}", avg_dp_size);
			
			LOG.info("Importing all {} data points into TSDB", points);
			importTimeValues(tsdb);
			
			final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
			LOG.info(String.format("Total: imported %d data points in %.3fs"
					+ " (%.1f points/s)",
					points, time_delta, (points / time_delta)));
		} finally {
			try {
				tsdb.shutdown().joinUninterruptibly();
			} catch (Exception e) {
				LOG.error("Unexpected exception", e);
				System.exit(1);
			}
		}
	}
	
	private static void importTimeValues(final TSDB tsdb) {
		final long start_time = System.nanoTime();
		long ping_start_time = start_time;

		int points = 0;
		try {
			for (final TimeValue tv : dataPoints) {
				importTimeValue(tsdb, tv);

				points++;
				if (points % 100000 == 0) {
					final long now = System.nanoTime();
					ping_start_time = (now - ping_start_time) / 100000;
					LOG.info(String.format("... %d data points in %dms (%.1f points/s)",
							points, ping_start_time,
							(1000000 * 1000.0 / ping_start_time)));
					ping_start_time = now;
				}
			}
		} catch (RuntimeException e) {
			LOG.error("Exception caught while importing data points into TSDB");
			throw e;
		}

		final long time_delta = (System.nanoTime() - start_time) / 1000000;
		LOG.info(String.format("Processed %d data points in %d ms (%.1f points/s)",
				points, time_delta, (points * 1000.0 / time_delta)));
	}

	private static int importFile(final String path) throws IOException {
		final long start_time = System.nanoTime();
		long ping_start_time = start_time;
		final BufferedReader in = open(path);
		String line = null;
		int points = 0;
		try {
			while ((line = in.readLine()) != null) {
				preprocessLineWords(Tags.splitString(line, ' '));

				points++;
				if (points % 100000 == 0) {
					final long now = System.nanoTime();
					ping_start_time = (now - ping_start_time) / 100000;
					LOG.info(String.format("... %d data points in %dms (%.1f points/s), freemem: %d",
							points, ping_start_time,
							(1000000 * 1000.0 / ping_start_time), 
							Runtime.getRuntime().freeMemory()));
					ping_start_time = now;
				}
			}
		} catch (RuntimeException e) {
			LOG.error("Exception caught while processing file "
					+ path + " line=" + line);
			throw e;
		} finally {
			in.close();
		}
		final long time_delta = (System.nanoTime() - start_time) / 1000000;
		LOG.info(String.format("Processed %s in %d ms, %d data points"
				+ " (%.1f points/s)",
				path, time_delta, points,
				(points * 1000.0 / time_delta)));
		return points;
	}

	private static void preprocessLineWords(String[] words) {
		final String metric = words[0];
		if (metric.length() <= 0) {
			throw new RuntimeException("invalid metric: " + metric);
		}
		long timestamp = Tags.parseLong(words[1]);
		if (timestamp <= 0) {
			throw new RuntimeException("invalid timestamp: " + timestamp);
		}

		final String value = words[2];
		if (value.length() <= 0) {
			throw new RuntimeException("invalid value: " + value);
		}

		// build a key that can be later be parsed to extract metric+tags
		StringBuilder mtBuf = new StringBuilder();
		mtBuf.append(metric);
		for (int i = 3; i < words.length; i++) {
			mtBuf.append(" ");
			mtBuf.append(words[i]);
		}

		final String metricTag = mtBuf.toString();

		int metricTagsId = metricTags.indexOf(metricTag);
		if (metricTagsId < 0) {
			metricTagsId = metricTags.size();
			metricTags.add(metricTag);
		}

		dataPoints.add(new TimeValue(metricTagsId, timestamp, value));
	}

	private static void importTimeValue(final TSDB tsdb, TimeValue tv) {
		final String mts = metricTags.get(tv.metricTagsId);

		final String[] mts_words = Tags.splitString(mts, ' ');
		final String metric = mts_words[0];

		final HashMap<String, String> tags = new HashMap<String, String>();
		for (int i = 1; i < mts_words.length; i++) {
			Tags.parse(tags, mts_words[i]);
		}

		CachedBatches.addPoint(tsdb, metric, tv.timestamp, tv.value, tags);
	}

	/**
	 * Opens a file for reading, handling gzipped files.
	 * @param path The file to open.
	 * @return A buffered reader to read the file, decompressing it if needed.
	 * @throws IOException when shit happens.
	 */
	private static BufferedReader open(final String path) throws IOException {
		InputStream is = new FileInputStream(path);
		if (path.endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		// I <3 Java's IO library.
		return new BufferedReader(new InputStreamReader(is));
	}

//	private static byte[] getBytes(long value) {
//		if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
//			return new byte[] {(byte) value};
//		}
//		else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
//			return Bytes.fromShort((short) value);
//		}
//		else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
//			return Bytes.fromInt((int) value);
//		}
//		else {
//			return Bytes.fromLong(value);
//		}
//	}

	private static class TimeValue {
		public final int metricTagsId;
		public final long timestamp;
		public final String value;
		//  	public final byte[] v;
		//  	public final short flags;

		public TimeValue(int metricTagsId, long timestamp, String value) {
			this.metricTagsId = metricTagsId;
			this.timestamp = timestamp;
			this.value = value;

			//  		if (Tags.looksLikeInteger(value)) {
			//  			v = getBytes(Tags.parseLong(value));
			//    		flags = (short) (v.length - 1);  // Just the length.
			//  		} else {
			//  			final float fval = Float.parseFloat(value);
			//  			if (Float.isNaN(fval) || Float.isInfinite(fval)) {
			//  				throw new IllegalArgumentException("value is NaN or Infinite: " + value + " for timestamp=" + timestamp);
			//  			}
			//  			
			//  			flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
			//  			v = Bytes.fromInt(Float.floatToRawIntBits(fval));
			//  		}
		}
	}
}
