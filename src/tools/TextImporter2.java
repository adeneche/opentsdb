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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

	private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

	private static int numRepeats = 1;
	private static int numDuplicates = 1;
	private static String duplicateTag = "";

	private static final List<MetricTags> metricTags = new ArrayList<MetricTags>();

	/** Prints usage and exits. */
	private static void usage(final ArgP argp) {
		System.err.println("Usage: import2 [--repeat=NUM-REPEATS] [--duplicate=DUPLICATE-TAG-NAME:NUM-DUPLICATES] path [more paths]");
		System.err.print(argp.usage());
		System.err.println("This tool can directly read gzip'ed input files.");
		System.exit(-1);
	}
	
	private static boolean parseParams(ArgP argp) {

		// handle repeat argument
		if (argp.has("--repeat")) {
			final String repeat = argp.get("--repeat");
			if (repeat == null) {
				System.err.println("--repeat is missing NUM-REPEATS");
				return false;
			}
			
			try {
				numRepeats = Integer.parseInt(repeat);
			} catch (NumberFormatException e) {
				System.err.println("NUM-REPEATS is not a valid integer");
				return false;
			}
		}
		
		if (numRepeats < 1) {
			System.err.println("NUM-REPEATS must be greater than 0");
			return false;
		}

		if (argp.has("--duplicate")) {
			final String duplicate = argp.get("--duplicate");
			if (duplicate == null) {
				System.err.println("--duplicate is missing DUPLICATE-TAG-NAME:NUM-DUPLICATES");
				return false;
			}

			final int idx = duplicate.indexOf(':');
			if (idx == 0) {
				System.err.println("--duplicate is missing DUPLICATE-TAG-NAME");
				return false;
			} else if (idx == -1 || idx == duplicate.length()) {
				System.err.println("--duplicate is missing NUM-DUPLICATES");
				return false;
			}

			duplicateTag = duplicate.substring(0, idx);
			
			try {
				Tags.validateString("DUPLICATE-TAG-NAME", duplicateTag);
			} catch (IllegalArgumentException e) {
				System.err.println("DUPLICATE-TAG-NAME is not a valid tag name");
				return false;
			}
			
			try {
				numDuplicates = Integer.parseInt(duplicate.substring(idx+1));
			} catch (NumberFormatException e) {
				System.err.println("NUM-DUPLICATES is not a valid integer");
				return false;
			}
		}

		if (numDuplicates < 1) {
			System.err.println("NUM-DUPLICATES must be greater than 0");
			return false;
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);
		argp.addOption("--repeat", "NUM-REPEATS", "(default 1) repeat all concatenated files NUM-REPEATS times"); 
		argp.addOption("--duplicate", "DUPLICATE-TAG-NAME:NUM-DUPLICATES", "duplicate each data points NUM-DUPLICATES while using TAG-NAME as a tag");
		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1 || !parseParams(argp)) {
			usage(argp);
		}

		LOG.info("repeat num: {}", numRepeats);
		LOG.info("duplicates num: {} and tag {}", numDuplicates, duplicateTag);
		LOG.info("paths: {}", Arrays.toString(args));		
		
		List<FileData> files = preloadFiles(args);

		// get a config object
		TSDB tsdb = null;

		Config config = CliOptions.getConfig(argp);
		argp = null;
		tsdb = new TSDB(config);
		tsdb.checkNecessaryTablesExist().joinUninterruptibly();

		// compute repetition duration
		long repDuration = 0;
		for (final FileData fd : files) {
			repDuration += fd.getDuration();
		}

		long start_file;
		long start_time;

		try {
			start_file = files.get(0).t0;

			for (final FileData fd : files) {
				// do not bother load in-memory if there is no repetition
				if (numRepeats == 1) {
					LOG.info("Importing file {} straight into TSDB", fd.path);

					start_time = System.nanoTime();

					fd.startAt(start_file);
					importFile(tsdb, fd, false);

					displayAvgSpeed(start_time, fd.size);
				} else {
					LOG.info("Loading file {} into memory ", fd.path);

					start_time = System.nanoTime();

					final TimeValue[] dataPoints = importFile(tsdb, fd, true);

					displayAvgSpeed(start_time, fd.size);

					LOG.info("Importing {} repetitions into TSDB", numRepeats);

					start_time = System.nanoTime();

					for (int rep = 0; rep < numRepeats; rep++) {
						fd.startAt(start_file + rep * repDuration);

						for (final TimeValue dp : dataPoints) {
							importDataPoint(tsdb, dp, fd);
						}
					}

					displayAvgSpeed(start_time, fd.size * numRepeats);

					start_file += fd.getDuration();
				}

				// we don't need to share the metricTags between files
				metricTags.clear();
			}
		} finally {
			try {
				tsdb.shutdown().joinUninterruptibly();
			} catch (Exception e) {
				LOG.error("Unexpected exception", e);
				System.exit(1);
			}
		}
	}

	private static void displayAvgSpeed(final long start_time, final int points) {
		final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
		LOG.info(String.format("Average speed: %d data points in %.3fs (%.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	/**
	 * parses all given files, computing the corresponding FileData objects. files that contain less than 2 datapoints are ignored
	 * 
	 * @param paths
	 * @return list of FileData objects
	 * @throws IOException
	 */
	private static List<FileData> preloadFiles(String[] paths) throws IOException {
		List<FileData> files = new ArrayList<FileData>();
		String line = null;
		String last = null;
		int points;
		long t0, t1;

		for (String path : paths) {
			final BufferedReader in = open(path);

			try {
				// process first line to extract t0
				line = in.readLine();
				last = null;

				if (line == null) { // empty file, ignore
					LOG.warn("file {} will be ignored because it's empty", path);
					in.close();
					continue;
				}

				t0 = Long.parseLong(Tags.splitString(line, ' ')[1]);
				points = 1;

				// TODO assuming all data points of a file have the same tags, check if duplicate tag isn't in the first line

				while ((line = in.readLine()) != null) {
					last = line;
					points++;
				}

				// process last line
				if (last == null) { // file contains one data point only
					// ignore because we can't compute a proper interval
					LOG.warn("file {} will be ignored because it contains one data point only", path);
					in.close();
					continue;
				}

				t1 = Long.parseLong(Tags.splitString(last, ' ')[1]);

				final FileData fd = new FileData(path, t0, t1, points);
				LOG.info("file {}: t0= {}, t1= {}, size={}, interval={}, duration={}", fd.path, fd.t0, fd.t1, fd.size, fd.getInterval(), fd.getDuration());
				files.add(fd);
			} catch (RuntimeException e) {
				LOG.error("Exception caught while processing file "	+ path + " line=" + line);
				throw e;
			} finally {
				in.close();
			}

		}

		return files;
	}
	/**
	 * Imports a given file into: memory if inMem is true or to TSDB
	 * @return array of data points if inMem is true, null otherwise
	 * @throws IOException
	 */
	private static TimeValue[] importFile(final TSDB tsdb, final FileData fd, boolean inMem) throws IOException {

		final BufferedReader in = open(fd.path);
		String line = null;
		TimeValue[] dataPoints = null;
		int points = 0;

		if (inMem) {
			dataPoints = new TimeValue[fd.size];
		}

		try {
			while ((line = in.readLine()) != null) {
				final String[] words = Tags.splitString(line, ' ');
				final TimeValue dp = processLine(words);

				if (inMem)
					dataPoints[points++] = dp;
				else
					importDataPoint(tsdb, dp, fd);
			}
		} catch (RuntimeException e) {
			LOG.error("Exception caught while processing file "
					+ fd.path + " line=" + line);
			throw e;
		} finally {
			in.close();
		}

		return dataPoints;
	}

	private static TimeValue processLine(final String[] words) {
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

		return new TimeValue(metricTagsId, timestamp, value);
	}

	/**
	 * Import a datapoint into TSDB using CachedBatches. Applies concatenation+repetition offset and generates duplicates if necessary 
	 */
	private static void importDataPoint(final TSDB tsdb, TimeValue dp, final FileData fd) {

		final MetricTags mts = metricTags.get(dp.metricTagsId);

		final long timestamp = fd.offsetTime(dp.timestamp);

		final HashMap<String, String> tags = new HashMap<String, String>(mts.tags);

		for (int d = 0; d < numDuplicates; d++) {
			if (numDuplicates > 1) {
				tags.put(duplicateTag, String.valueOf(d));
			}

			CachedBatches.addPoint(tsdb, mts.metric, timestamp, dp.value, tags);
		}
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

	/**
	 * converts a timestamp to millis if it is in seconds
	 * @return
	 */
	private static long toMillis(long t) {
		if (isMillis(t)) return t;

		return t * 1000;
	}

	private static boolean isMillis(long t) {
		return (t & Const.SECOND_MASK) != 0;
	}

	/**
	 * Contains time informations about a given file. Used to properly offset the data
	 */
	static class FileData {
		public final String path;
		private final boolean ms;
		public final long t0; // timestamp of first event in file
		public final long t1; // timestamp of last event in file
		public final int size; // num data points in file

		private long offset;

		/**
		 * average interval between two data points
		 */
		public long getInterval() {
			return (t1 - t0) / (size - 1); 
		}

		/**
		 * time duration between t0 and start of next concatenated file 
		 */
		public long getDuration() {
			return t1 - t0 + getInterval();
		}

		/**
		 * Computes time offset so that this file starts at time t
		 * @param t starting time
		 */
		public void startAt(long t) {
			if (ms) {
				// this file uses milliseconds, convert t to milliseconds (if needed)
				t = toMillis(t);
			}
			else if (isMillis(t)) {
				// this file uses seconds and t is in milliseconds
				// convert t to seconds
				t = (long) Math.ceil(t / 1000.0);
			}

			offset = t - t0;
		}

		/**
		 * offsets given time according to this file's offset
		 */
		public long offsetTime(long t) {
			// we assume all data points of a file have the same format (seconds vs milliseconds) 
			return t + offset;
		}

		public FileData(final String path, final long t0, final long t1, final int size) {
			this.path = path;
			ms = isMillis(t0) || isMillis(t1);
			this.t0 = ms ? toMillis(t0) : t0;
			this.t1 = ms ? toMillis(t1) : t1;
			this.size = size;
		}
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

	private static class TimeValue {
		public final int metricTagsId;
		public final long timestamp;
		public final String value;

		public TimeValue(final int metricTagsId, final long timestamp, final String value) {
			this.metricTagsId = metricTagsId;
			this.timestamp = timestamp;
			this.value = value;
		}
	}


}
