// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author jscott
 */
public class CachedBatches {

	  private static final Logger LOG = LoggerFactory.getLogger(CachedBatches.class);

	private static final Map<String, Batch> batches = new ConcurrentHashMap<String, Batch>();
	private static final Timer TIMER = new Timer(true);
	private static final TimerTask TIMER_TASK = new TimerTask() {
		@Override
		public void run() {
			rebaseBatches();
		}
	};

	/**
	 * Schedule the Timer to run once per minute.
	 */
	static {
		final long delay = 1000 * 60;
		TIMER.schedule(TIMER_TASK, delay, delay);
	}

	/**
	 * There should be no WAY to create instances of this.
	 */
	private CachedBatches() {
	}

	public static Deferred<Object> addPoint(final TSDB tsdb, final String metric, final long timestamp, final String value, final Map<String, String> tags) {
		return getBatch(tsdb, metric, timestamp, tags).addPoint(timestamp, value);
	}

	/**
	 * Blindly pull whatever cached DataPoints exist for the given metric and tags combination.
	 *
	 * @param metric name of the metric
	 * @param tags tags used against metric
	 * @return DataPoints for a maximum of one complete hour.
	 */
	public static DataPoints get(final String metric, final Map<String, String> tags) {
		Batch batch = batches.get(metric + tags.toString());
		return batch == null ? null : batch.getDataPoints();
	}

	/**
	 * Pull DataPoints for the given metric and tags combination, but only if the timestamp falls
	 * within the base hour for the DataPoints.
	 *
	 * @param metric name of the metric
	 * @param tags tags used against metric
	 * @param timestamp used to extract the base hour, may be millis or seconds
	 * @return DataPoints for a maximum of one complete hour.
	 */
	public static DataPoints get(final String metric, final long timestamp, final Map<String, String> tags) {
		Batch batch = batches.get(metric + tags.toString());
		DataPoints dp = null;
		if (batch != null && baseTime(timestamp) == batch.batchBaseTime()) {
			dp = batch.getDataPoints();
		}
		return dp;
	}

	/**
	 * Pull DataPoints for the given metric and tags combination, but only if the DataPoints that
	 * fall within the start and end times provided.
	 *
	 * @param metric name of the metric
	 * @param tags tags used against metric
	 * @param startTime The earliest time (inclusive) a DataPoint in the return set may start with,
	 * may be millis or seconds
	 * @param endTime The latest (inclusive) time a DataPoint in the return set may start with, may
	 * be millis or seconds
	 * @return DataPoints for a maximum of one complete hour. <code>null</code> if the time range
	 * doesn't match what is cached or if the combination doesn't exist.
	 */
	public static DataPoints get(final String metric, final Map<String, String> tags,
			final long startTime, final long endTime) {
		Batch batch = batches.get(metric + tags.toString());
		DataPoints dp = null;
		if (batch != null) {
			final long startBase = baseTime(startTime);
			final long endBase = baseTime(endTime);
			if (startBase == batch.baseTime || endBase == batch.baseTime
					|| (batch.baseTime > startBase && batch.baseTime < endBase)) {
				dp = batch.getDataPoints();
			}
		}
		return dp;
	}

	/**
	 * Returns the time in seconds format which is nearest preceding hour boundary.
	 *
	 * @param timestamp may be millis or seconds
	 * @return base hour in seconds
	 */
	public static long baseTime(final long timestamp) {
		long baseTime;
		if ((timestamp & net.opentsdb.core.Const.SECOND_MASK) != 0) {
			// drop the ms timestamp to seconds to calculate the base timestamp
			baseTime = ((timestamp / 1000) - ((timestamp / 1000) % net.opentsdb.core.Const.MAX_TIMESPAN));
		}
		else {
			baseTime = (timestamp - (timestamp % net.opentsdb.core.Const.MAX_TIMESPAN));
		}
		return baseTime;
	}

	private static Batch getBatch(final TSDB tsdb, final String metric, final long timestamp, final Map<String, String> tags) {
		Batch batch = batches.get(metric + tags.toString());
		if (batch == null) {
			batch = new Batch(timestamp, tsdb.newBatch(metric, tags));
			batches.put(metric + tags.toString(), batch);
		}
		return batch;
	}

	/**
	 * For batches that are rapidly changing this won't do much, but for batches that don't get
	 * updated that often this will cause them to persist near hour boundaries.
	 */
	private static void rebaseBatches() {
		LOG.info("rebase Batches");
		long base = baseTime(System.currentTimeMillis());
		for (Map.Entry<String, Batch> entry : batches.entrySet()) {
			entry.getValue().persistIfNecessary(base);
		}
	}

	synchronized static void shutdownHook() {
		for (Map.Entry<String, Batch> entry : batches.entrySet()) {
			entry.getValue().shutdown();
		}
	}

	/**
	 * No reason to expose this to the rest of the code base.
	 */
	private static class Batch {

		private final WritableDataPoints dataPoints;
		private long baseTime;

		Batch(final long timestamp, final net.opentsdb.core.WritableDataPoints dataPoints) {
			this.baseTime = baseTime(timestamp);
			this.dataPoints = dataPoints;
		}

		DataPoints getDataPoints() {
			return dataPoints;
		}

		long batchBaseTime() {
			return baseTime;
		}

		Deferred<Object> addPoint(final long timestamp, final String value) {
			persistIfNecessary(baseTime(timestamp));

			final Deferred<Object> def;
			if (net.opentsdb.core.Tags.looksLikeInteger(value)) {
				def = dataPoints.addPoint(timestamp, net.opentsdb.core.Tags.parseLong(value));
			}
			else {
				def = dataPoints.addPoint(timestamp, Float.parseFloat(value));
			}
			return def;
		}

		/**
		 * This is pretty much only in the case of a shutdown where data will no longer be coming
		 * in. Do not use this otherwise!
		 */
		void shutdown() {
			dataPoints.persist();
			baseTime = Long.MIN_VALUE;
		}

		/**
		 * Data comes in time order, and if the BASE we had doesn't match the current base we have
		 * hit a new hour. It is time to persist the records, reset the baseTime and continue adding
		 * data points.
		 */
		void persistIfNecessary(final long base) {
			if (base != baseTime) {
				dataPoints.persist();
				baseTime = base;
			}
		}
	}
}
