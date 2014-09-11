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

import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

	private static final List<MetricTags> metricTags = new ArrayList<MetricTags>();
	private static byte[] dp_bytes = null;
	private static int next_dp = 0;

	/** Prints usage and exits.  */
	static void usage(final ArgP argp) {
		System.err.println("Usage: import path [more paths] --noimport --noload --nomem");
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
		argp.addOption("--nomem", "do not display memory usage (avoid calling GC)");
		
		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1) {
			usage(argp);
		}

		// get a config object
		Config config = CliOptions.getConfig(argp);

		final boolean noLoad = argp.has("--noload");
		final boolean noImport = noLoad || argp.has("--noimport");
		final boolean noMem = argp.has("--nomem");
		argp = null;

		final TSDB tsdb = noImport ? null : new TSDB(config);

		if (tsdb != null) {
			tsdb.checkNecessaryTablesExist().joinUninterruptibly();			
		}

		try {
			// we start by computing how many data points contained in all files 
			int points = 0;
			for (final String path : args) {
				points += loadFile(path, true, false);
			}
			
			LOG.info("Files contain a total of {} data points", points);
			
			final long start_time = System.nanoTime();
			final Runtime runtime = Runtime.getRuntime();
			
			if (!noLoad) {
				if (!noMem) runtime.gc();
				final long usedmem = noMem ? 0 : runtime.totalMemory() -  runtime.freeMemory();
				
				dp_bytes = new byte[points*TimeValue.SIZE];

				for (final String path : args) {
					loadFile(path, false, noMem);
				}

				if (!noMem) {
					runtime.gc();
					
					final long avg_dp_size = ((runtime.totalMemory() - runtime.freeMemory()) - usedmem) / points;
					LOG.info("Average datapoint size = {}", avg_dp_size);
				}
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
			for (int i = 0; i < next_dp; i++) {
				final TimeValue dp = TimeValue.fromByteArray(dp_bytes, i*TimeValue.SIZE);
				importTimeValue(tsdb, dp);

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

	private static int loadFile(final String path, final boolean noLoad, final boolean noMem) throws IOException {
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
							noMem ? 0 : runtime.totalMemory() - runtime.freeMemory()));
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
		
		short metricTagsId = (short) metricTags.indexOf(mts);
		if (metricTagsId < 0) {
			metricTagsId = (short) metricTags.size();
			metricTags.add(mts);
		}

		TimeValue.toByteArray(dp_bytes, next_dp*TimeValue.SIZE, metricTagsId, timestamp, value);
		next_dp++;
	}

	private static void importTimeValue(final TSDB tsdb, final TimeValue dp) {
		
		final MetricTags mts = metricTags.get(dp.metricTagsId);
		final String value = dp.getValueString();
		
		CachedBatches.addPoint(tsdb, mts.metric, dp.timestamp, value, mts.tags);
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

	private static class TimeValue { // (2+8+4+1) = 15 bytes
		public static final int SIZE = 15; // in bytes
		public short metricTagsId;
		public final long timestamp;
		private final int ivalue;
		private final boolean isfloat;

		public String getValueString() {
			if (isfloat)
				return String.valueOf(Float.intBitsToFloat(ivalue));
			else
				return String.valueOf(ivalue);
		}
		
		public static TimeValue fromByteArray(final byte[] bytes, final int off) {
			int cur = off;
			final short metricTagsId = Bytes.getShort(bytes, cur); cur+= 2;
			final long timestamp = Bytes.getLong(bytes, cur); cur+= 8;
			final boolean isfloat = bytes[cur] == 1; cur++;
			final int ivalue = Bytes.getInt(bytes, cur); cur+= 4;
			
			return new TimeValue(metricTagsId, timestamp, isfloat, ivalue);
		}
		
		public static void toByteArray(final byte[] bytes, final int off, final short metricTagsId, final long timestamp, final String value) {
			final boolean isfloat = !Tags.looksLikeInteger(value);
			final int ivalue;
			if (isfloat) {
				float fval = Float.parseFloat(value);
				ivalue = Float.floatToRawIntBits(fval);
			} else {
				ivalue = Integer.parseInt(value);
			}
			
			int cur = off;
			System.arraycopy(Bytes.fromShort(metricTagsId), 0, bytes, off, 2); cur+= 2;
			System.arraycopy(Bytes.fromLong(timestamp), 0, bytes, cur, 8); cur+= 8;
			bytes[cur] = (byte) (isfloat ? 1:0); cur++;
			System.arraycopy(Bytes.fromInt(ivalue), 0, bytes, cur, 4); cur+= 4;
		}

		public TimeValue(final short metricTagsId, final long timestamp, final boolean isfloat, int ivalue) {
			this.metricTagsId = metricTagsId;
			this.timestamp = timestamp;
			this.ivalue = ivalue;
			this.isfloat = isfloat;
		}
	}

}
