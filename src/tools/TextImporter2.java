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
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.Const;
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
	
	private static BufferedWriter output = null;
	private static SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");

	/** Prints usage and exits. */
	private static void usage(final ArgP argp) {
		System.err.println("Usage: import2 [repeat NUM-REPEATS] [duplicate NUM-DUPLICATES DUPLICATE-TAG-NAME] path [more paths] --tofile=PATH");
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

	public static void main(String[] args) throws Exception {
		ArgP argp = new ArgP();
		CliOptions.addCommon(argp);
		CliOptions.addAutoMetricFlag(argp);
		argp.addOption("--tofile", "FILE-NAME", "export data points into a file instead of TSDB");

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

		final String outputPath = argp.get("--tofile");
		

		List<FileData> files = preloadFiles(args);
		
//		System.exit(0);

		// get a config object
		TSDB tsdb = null;
		
		if (outputPath == null) {
			Config config = CliOptions.getConfig(argp);
			argp = null;
			tsdb = new TSDB(config);
			tsdb.checkNecessaryTablesExist().joinUninterruptibly();
		} else {
			OutputStream os = new FileOutputStream(outputPath);
			output = new BufferedWriter(new OutputStreamWriter(os));
		}

		// compute repetition duration
		long repDuration = 0;
		for (final FileData fd : files) {
			repDuration += fd.getDuration();
		}
		
		long start_file;
		
		try {
			start_file = files.get(0).t0;
			
			for (final FileData fd : files) {
				// load file data points into memory [we won't need this if there is no repetition]
				
				for (int rep = 0; rep < numRepeats; rep++) {
					fd.startAt(start_file + rep * repDuration);
					
					importFile(tsdb, fd);					
				}
				
				start_file += fd.getDuration();
				
			}

		} finally {
			if (tsdb != null) {
				try {
					tsdb.shutdown().joinUninterruptibly();
				} catch (Exception e) {
					LOG.error("Unexpected exception", e);
					System.exit(1);
				}
			} else {
				output.close();
			}
		}
	}

	private static void importFile(final TSDB tsdb, final FileData fd) throws IOException {

		final BufferedReader in = open(fd.path);
		String line = null;

		try {
			while ((line = in.readLine()) != null) {
				processLine(tsdb, Tags.splitString(line, ' '), fd);
			}
		} catch (RuntimeException e) {
			LOG.error("Exception caught while processing file "
					+ fd.path + " line=" + line);
			throw e;
		} finally {
			in.close();
		}
	}

	private static void processLine(final TSDB tsdb, final String[] words, final FileData fd) throws IOException {
		final String metric = words[0];
		if (metric.length() <= 0) {
			throw new RuntimeException("invalid metric: " + metric);
		}
		long timestamp = Tags.parseLong(words[1]);
		if (timestamp <= 0) {
			throw new RuntimeException("invalid timestamp: " + timestamp);
		}

		timestamp = fd.offsetTime(timestamp);

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

		for (int d = 0; d < numDuplicates; d++) {
			if (numDuplicates > 1) {
				tags.put(duplicateTag, String.valueOf(d));
			}
			
			if (tsdb != null) {
				CachedBatches.addPoint(tsdb, metric, timestamp, value, tags);
			} else {
				StringBuilder sb = new StringBuilder();
				sb.append(metric)
				.append(' ')
				.append(sdf.format(new Date(toMillis(timestamp))))
				.append(' ')
				.append(value);
				for (String key : tags.keySet()) {
					sb.append(' ')
					.append(key)
					.append('=')
					.append(tags.get(key));
				}
				sb.append('\n');
				
				output.write(sb.toString());
			}
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
	
	private static long toMillis(long t) {
		if (isMillis(t)) return t;

		return t * 1000;
	}
	
	private static boolean isMillis(long t) {
		return (t & Const.SECOND_MASK) != 0;
	}

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

}
