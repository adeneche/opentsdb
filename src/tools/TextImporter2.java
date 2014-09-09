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
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

	private static final List<MetricTags> metricTags = new ArrayList<MetricTags>();
	private static TimeValue[] dataPoints = null;
	private static int next_dp = 0;

	/** Prints usage and exits.  */
	static void usage(final ArgP argp) {
		System.err.println("Usage: import path [more paths] --noimport --noload");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);
		argp.addOption("--noimport", "do not load the data points into openTSDB");
		argp.addOption("--noload", "do not load the data points into memory");
		
		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1) {
			usage(argp);
		}

		// get a config object
		Config config = CliOptions.getConfig(argp);

		final boolean noLoad = argp.has("--noload");
		final boolean noImport = noLoad || argp.has("--noimport");
		argp = null;

		final TSDB tsdb = noImport ? null : new TSDB(config);

		if (tsdb != null) {
			tsdb.checkNecessaryTablesExist().joinUninterruptibly();			
		}

		try {
			// we start by computing how many data points contained in all files 
			int points = 0;
			for (final String path : args) {
				points += loadFile(path, true);
			}
			
			LOG.info("Files contain a total of {} data points", points);
			
			final long start_time = System.nanoTime();
			final Runtime runtime = Runtime.getRuntime();
			
			if (!noLoad) {
				runtime.gc();
				final long usedmem = runtime.totalMemory() -  runtime.freeMemory();
				
				dataPoints = new TimeValue[points];

				for (final String path : args) {
					loadFile(path, false);
				}

				runtime.gc();
				final long avg_dp_size = ((runtime.totalMemory() - runtime.freeMemory()) - usedmem) / points;
				LOG.info("Average datapoint size = {}", avg_dp_size);
			}
			
			if (tsdb != null) {
				LOG.info("Importing all {} data points into TSDB", points);
				importTimeValues(tsdb);
				
				final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
				LOG.info(String.format("Total: imported %d data points in %.3fs"
						+ " (%.1f points/s)",
						points, time_delta, (points / time_delta)));
			}
		} finally {
			if (tsdb != null) {
				try {
					tsdb.shutdown().joinUninterruptibly();
				} catch (Exception e) {
					LOG.error("Unexpected exception", e);
					System.exit(1);
				}
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
				if (points % 1000000 == 0) {
					final long now = System.nanoTime();
					ping_start_time = (now - ping_start_time) / 1000000;
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

	private static int loadFile(final String path, final boolean noLoad) throws IOException {
		final long start_time = System.nanoTime();
		long ping_start_time = start_time;
		final BufferedReader in = open(path);
		final Runtime runtime = Runtime.getRuntime();
		
		String line = null;
		int points = 0;
		
		try {
			while ((line = in.readLine()) != null) {
				points++;
				
				if (noLoad) continue;

				preprocessLineWords(Tags.splitString(line, ' '));

				if (points % 1000000 == 0) {
					final long now = System.nanoTime();
					ping_start_time = (now - ping_start_time) / 1000000;
					LOG.info(String.format("... %d data points in %dms (%.1f points/s), usedmem: %d",
							points, ping_start_time,
							(1000000 * 1000.0 / ping_start_time), 
							runtime.totalMemory() - runtime.freeMemory()));
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

		final HashMap<String, String> tags = new HashMap<String, String>();
		for (int i = 3; i < words.length; i++) {
			Tags.parse(tags, words[i]);
		}

		final MetricTags mts = new MetricTags(metric, tags);
		
		int metricTagsId = metricTags.indexOf(mts);
		if (metricTagsId < 0) {
			metricTagsId = metricTags.size();
			metricTags.add(mts);
		}

		dataPoints[next_dp++] = new TimeValue(metricTagsId, timestamp, value);
	}

	private static void importTimeValue(final TSDB tsdb, TimeValue tv) {
		final MetricTags mts = metricTags.get(tv.metricTagsId);

		CachedBatches.addPoint(tsdb, mts.metric, tv.timestamp, tv.value, mts.tags);
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

	private static class MetricTags {
		public final String metric;
		public final Map<String, String> tags;
		
		public MetricTags(final String metric, final Map<String, String> tags) {
			this.metric = metric;
			this.tags = tags;
		}

		@Override
		public int hashCode() {
			return (metric + tags).hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof MetricTags))
				return false;
			MetricTags mts = (MetricTags)obj;
			return metric.equals(mts.metric) && tags.equals(mts.tags);
		}
		
		
	}
	
	private static class TimeValue {
		public final int metricTagsId;
		public final long timestamp;
		public final String value;
		//  	public final byte[] v;
		//  	public final short flags;

		public TimeValue(final int metricTagsId, final long timestamp, final String value) {
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
