// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details. You should have received a copy
// of the GNU Lesser General Public License along with this program. If not,
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);
	
	private static boolean toScreen;

	/** Prints usage and exits. */
	private static void usage(final ArgP argp) {
		System.err.println("Usage: import2 path [more paths] [--noimport] [--print]");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(-1);
	}
	
	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);
		argp.addOption("--print", "print data points on screen");
		argp.addOption("--noimport", "do not import data to TSDB");
		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1) {
			usage(argp);
		}

		LOG.info("paths: {}", Arrays.toString(args));		
		
		toScreen = argp.has("--print");

		final long start_time = System.nanoTime();
		int points = 0;
		// get a config object
		TSDB tsdb = null;

		if (!argp.has("--noimport")) {
			Config config = CliOptions.getConfig(argp);
			argp = null;
			tsdb = new TSDB(config);
			tsdb.checkNecessaryTablesExist().joinUninterruptibly();
		}

		try {
			for (final String path : args) {
				LOG.info("Importing file {}", path);
				points += importFile(tsdb, path);
			}
			
			displayAvgSpeed(start_time, points);
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

	private static void displayAvgSpeed(final long start_time, final int points) {
		final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
		LOG.info(String.format("Average speed: %d data points in %.3fs (%.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	/**
	 * Imports a given file to TSDB
	 * @return number of points imported from file
	 * @throws IOException
	 */
	private static int importFile(final TSDB tsdb, final String path) throws IOException {

		final BufferedReader in = open(path);
		String line = null;

		int points = 0;

		final long start_time = System.nanoTime();
		
		try {
			while ((line = in.readLine()) != null) {
				final String[] words = Tags.splitString(line, ' ');
				processAndImportLine(tsdb, words);

				points++;
				
				if (points % 1000000 == 0) {
					displayAvgSpeed(start_time, points);
				}
			}
		} catch (RuntimeException e) {
			LOG.error("Exception caught while processing file " + path + " line=" + line);
			throw e;
		} finally {
			in.close();
		}

		return points;
	}

	private static void processAndImportLine(final TSDB tsdb, final String[] words) {
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

		if (tsdb != null) {
			CachedBatches.addPoint(tsdb, metric, timestamp, value, tags);
		} 

		if (toScreen) {
			logDataPoint(metric, timestamp, value, tags);
		}
	}
	
	private static void logDataPoint(final String metric, final long timestamp, final String value, final HashMap<String, String> tags) {
		final StringBuilder buf = new StringBuilder();

		buf.append(metric)
		.append(' ')
		.append(DumpSeries.date(timestamp))
		.append(' ')
		.append(value);
		
		for (String tag : tags.keySet()) {
			buf.append(' ')
			.append(tag)
			.append('=')
			.append(tags.get(tag));
		}
		
		LOG.info(buf.toString());
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
