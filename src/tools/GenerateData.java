package net.opentsdb.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateData {
	private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);

	/** Prints usage and exits.  */
	static void usage(final ArgP argp) {
		System.err.println("Usage: generate --metric=NAME --year=YYY");
		System.err.print(argp.usage());
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
		ArgP argp = new ArgP();
		argp.addOption("--metric", "NAME", "Output file name basis");
		argp.addOption("--num-metrics", "METRICS", "(default 1) number of metrics generated");
		argp.addOption("--num-tagk", "NUM_TAGK", "(default 1) number of tags for each data point");
		argp.addOption("--num-tagv", "NUM_TAGV", "(default 1) number of different values generated for each tag");
		argp.addOption("--year", "YYYY", "starting year");
		argp.addOption("--num-years", "YEARS", "(default 1) number of years generated");
		argp.addOption("--num-days", "DAYS",  "(default 1) total number of days to generate per year");
		argp.addOption("--pph", "SAMPLE_SIZE", "(default 3600) number of data points per hour");
		argp.addOption("--range", "RANGE", "(default 101) value += random(range) - gap");
		argp.addOption("--gap", "GAP", "(default 50) value += random(range) - gap");
		argp.addOption("--compress", "[optional] compress generated file");
		argp.addOption("--csv", "[optional] generate CSV file");
		if (args.length < 2) {
			usage(argp);
		}

		args = CliOptions.parse(argp, args);
		if (!argp.has("--metric") || !argp.has("--year")) {
			usage(argp);
		}

		String metricName = argp.get("--metric");
		int numMetrics = Integer.parseInt(argp.get("--num-metrics", "1"));
		int numTagK = Integer.parseInt(argp.get("--num-tagk", "1"));
		int numTagV = Integer.parseInt(argp.get("--num-tagv", "1"));
		int startYear = Integer.parseInt(argp.get("--year"));
		int totalYears = Integer.parseInt(argp.get("--num-years", "1"));
		int days = Integer.parseInt(argp.get("--num-days", "1"));
		boolean useCompression = argp.has("--compress");
		int pph = Integer.parseInt(argp.get("--pph", "3600"));
		int range = Integer.parseInt(argp.get("--range", "101"));
		int gap = Integer.parseInt(argp.get("--gap", "50"));
		boolean csv = argp.has("--csv");
		
		if (pph <= 0) {
			System.err.println("pph must be a positive number");
			System.exit(-1);
		}

		LOG.info("Generating {} years starting from year {}, for metric {} with {} points per hour", totalYears, startYear, metricName, pph);
		LOG.info("Generating {} metrics with {} tags and {} values for each tag", numMetrics, numTagK, numTagV);

		generateYearlyFiles(useCompression, metricName, numMetrics, numTagK, numTagV, startYear, totalYears, days, pph, range, gap, new Random(), csv);
	}

	public static void generateYearlyFiles(final boolean useCompression, final String metricName, final int numMetrics, final int numTagK, final int numTagV, final int startYear, final int totalYears, final int days, final int pph, final int range, final int gap, final Random rand, final boolean csv) throws IOException {
		//TODO add timezone (--tz) param to argp
		Calendar cal = Calendar.getInstance(); // use local timezone
		cal.set(startYear, 0, 1, 0, 0, 0);

		String extension = csv ? ".csv" : ".tsd";
		if (useCompression) extension += ".gz";

		long value = rand.nextInt(range) - gap;
		long count = 0;
		
		int[] tagValues = new int[numTagK]; 

		long startTime = System.currentTimeMillis();

		for (int year = startYear; year < startYear + totalYears; year++) {
			LOG.info("generating year {} / {}", year, totalYears);

			File metricFile = new File("" + year + "-" + metricName + extension);
			OutputStream os = createOutputStream(useCompression, metricFile);

			long time = (pph > 3600) ? cal.getTimeInMillis() : cal.getTimeInMillis() / 1000;
			int time_inc = (pph > 3600) ? 3600000 / pph : 3600 / pph;
			
				for (int day = 0; day < days; day++) {
					LOG.info("Day {} / {}", day+1, days);
					for (int hour = 0; hour < 24; hour++) {
						for (int i = 0; i < pph; i++) {
							
							for (int v = 0; v < numTagK; v++) {
								tagValues[v] = rand.nextInt(numTagV);
							}
							
							final String mname = metricName + ((numMetrics > 1) ? "." + rand.nextInt(numMetrics) : "");
							
							if (csv)
								writeRecordCSV(os, mname, time, value, tagValues);
							else
								writeRecord(os, mname, time, value, tagValues);
							
							// Alter the value by a range of +/- RANDOM_GAP
							value += rand.nextInt(range) - gap;
							
							time+= time_inc;
							
							count++;
						}
					}
				}

			os.flush();
			os.close();

			cal.add(Calendar.YEAR, 1);
		}
		long totalTime = System.currentTimeMillis() - startTime;
		//TODO display total number of data points
		LOG.info("Total time to create {} data points: {}ms", count, totalTime);
	}

	private static OutputStream createOutputStream(boolean useCompression, File path) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);
		return useCompression
				? new BufferedOutputStream(new GZIPOutputStream(fos))
		: new BufferedOutputStream(fos);
	}

	private static void writeRecord(OutputStream os, String metricName, long time, long value, int[] tagValues) throws IOException {
		StringBuffer record = new StringBuffer();
		record.append(metricName)
		.append(" ")
		.append(time)
		.append(" ")
		.append(value);
		
		for (int v = 0; v < tagValues.length; v++) {
			record.append(" ")
			.append("tag")
			.append(v)
			.append("=")
			.append("value")
			.append(tagValues[v]);
		}
		
		record.append("\n");
		
		os.write(record.toString().getBytes());
	}

	private static void writeRecordCSV(final OutputStream os, final String metricName, final long time, final long value, final int[] tagValues) throws IOException {
		StringBuffer record = new StringBuffer();
		record.append('"').append(metricName).append('"')
		.append(", ")
		.append(time)
		.append(", ")
		.append(value);
		
		for (int v = 0; v < tagValues.length; v++) {
			record.append(", \"")
			.append("tag")
			.append(v)
			.append("=")
			.append("value")
			.append(tagValues[v])
			.append('"');
		}
		
		record.append("\n");
		
		os.write(record.toString().getBytes());
	}
}
