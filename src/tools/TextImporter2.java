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
import java.util.Arrays;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

	private static int numRepeats = 1;
	private static int numDuplicates = 1;
	private static String duplicateTag = "";
	
	/** Prints usage and exits.  */
	private static void usage(final ArgP argp) {
		System.err.println("Usage: import2 [repeat NUM-REPEATS] [duplicate NUM-DUPLICATES DUPLICATE-TAG-NAME] path [more paths]");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(-1);
	}
	
	private static String[] parseParams(String[] args) {
		int cur = 0;
		
			// check if first param is "repeat num_repeats"
			if ("repeat".equals(args[0])) {
				if (args.length < 3) {
					throw new RuntimeException("Not enough arguments");
				}
				
				numRepeats = Integer.parseInt(args[1]);
				if (numRepeats <= 0) {
					throw new IllegalArgumentException("NUM-REPEATS must be greater than 0");
				}
				
				cur += 2;
			}
			
			// check if next param is "duplicate num_duplicates duplicate_tag_name"
			if ("duplicate".equals(args[cur])) {
				if (args.length - cur < 4) {
					throw new RuntimeException("Not enough arguments");
				}
				
				numDuplicates = Integer.parseInt(args[cur+1]);
				if (numDuplicates <= 0) {
					throw new IllegalArgumentException("NUM-DUPLICATES must be greater than 0");
				}
				
				duplicateTag = args[cur+2];
				Tags.validateString("DUPLICATE-TAG-NAME", duplicateTag);
				
				cur += 3;
			}
			
			// return unparsed arguments
			String[] unparsed = new String[args.length - cur];
			for (int i = 0; cur < args.length; cur++, i++) {
				unparsed[i] = args[cur];
			}
			
			return unparsed;
	}

	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);

		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1) {
			usage(argp);
		}
		
		try {
			args = parseParams(args);
		} catch (Exception e) {
			usage(argp);
		}
		
		LOG.info("repeat num: {}", numRepeats);
		LOG.info("duplicates num: {} and tag {}", numDuplicates, duplicateTag);
		LOG.info("paths: {}", Arrays.toString(args));
		
		System.exit(1);

		// get a config object
		Config config = CliOptions.getConfig(argp);

		final TSDB tsdb = new TSDB(config);
		tsdb.checkNecessaryTablesExist().joinUninterruptibly();
		argp = null;
		try {
			int points = 0;
			final long start_time = System.nanoTime();

			for (final String path : args) {
				points += importFile(tsdb.getClient(), tsdb, path);
			}

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

	private static int importFile(final HBaseClient client,
			final TSDB tsdb,
			final String path) throws IOException {

		final long start_time = System.nanoTime();
		long ping_start_time = start_time;
		final BufferedReader in = open(path);
		String line = null;
		int points = 0;
		try {
			while ((line = in.readLine()) != null) {
				final String[] words = Tags.splitString(line, ' ');
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
					if (!words[i].isEmpty()) {
						Tags.parse(tags, words[i]);
					}
				}

				CachedBatches.addPoint(tsdb, metric, timestamp, value, tags);
				points++;
				if (points % 1000000 == 0) {
					final long now = System.nanoTime();
					ping_start_time = (now - ping_start_time) / 1000000;
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

}
