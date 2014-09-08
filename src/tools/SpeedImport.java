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
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.Tags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing how fast reading big csv files can be
 * 
 * @author adeneche
 *
 */
final class SpeedImport {

	private static final Logger LOG = LoggerFactory.getLogger(SpeedImport.class);

	/** Prints usage and exits with the given retval.  */
	static void usage(ArgP argp) {
		System.err.println("Usage: import [--process] path [more paths]");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		argp.addOption("--process", "should we process each line ?");

		if (args.length == 0) {
			usage(argp);
		}

		int points = 0;
		final long start_time = System.nanoTime();

		args = CliOptions.parse(argp, args);

		for (final String path : args) {
			points += importFile(path, argp.has("--process"));
		}

		final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
		LOG.info(String.format("Total: imported %d data points in %.3fs"
				+ " (%.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	private static int importFile(final String path, boolean process) throws IOException {
		final long start_time = System.nanoTime();
		long ping_start_time = start_time;
		final BufferedReader in = open(path);
		String line = null;
		int points = 0;
		try {
			while ((line = in.readLine()) != null) {
				if (process) {
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
				}

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
