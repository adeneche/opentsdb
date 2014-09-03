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

import net.opentsdb.core.CachedBatches;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

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
    argp.addOption("--offset", "time offset");
    argp.addOption("--repeat_count", "number of times loaded data is repeated");
    argp.addOption("--repeat_offset", "time offset each time a file is repeated");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, 1);
    } else if (args.length < 1) { //TODO change this to account for our own parameters
      usage(argp, 2);
    }
    
    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    final long offset = parseDuration(argp.get("--offset"));
    final int repeatCount = parseRepeatCount(argp.get("--repeat_count", "1"));
    final long repeatOffset = repeatCount > 1 ? parseDuration(argp.get("--repeat_offset")) : 0;
    //TODO exception if repeatCount > 1 AND repeatOffset == 0
    
    LOG.info("time_offset: "+offset);
    LOG.info("repeat_count: "+repeatCount);
    LOG.info("repeat_offset: "+repeatOffset);
    
    final TSDB tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;
    try {
      int points = 0;
      final long start_time = System.nanoTime();
      
      for (int repeat = 0; repeat < repeatCount; repeat++) {
          for (final String path : args) {
              points += importFile(tsdb.getClient(), tsdb, path, offset + repeat * repeatOffset);
            }
      }
      
      final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
      LOG.info(String.format("Total: imported %d data points in %.3fs"
                             + " (%.1f points/s)",
                             points, time_delta, (points / time_delta)));
      CachedBatches.shutdown();
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
                                final String path,
                                final long offset) throws IOException {
	LOG.info("importing file with offset: "+offset);
	
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

        if ((timestamp & Const.SECOND_MASK) == 0) { // time is in seconds
        	timestamp *= 1000; // convert to ms
        }

        timestamp += offset;
        
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
