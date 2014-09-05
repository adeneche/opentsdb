package net.opentsdb.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateData {
	private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);

//	private static final int RANDOM_RANGE = 101;
//	private static final int RANDOM_GAP = 50;


	/** Prints usage and exits.  */
	static void usage(final ArgP argp) {
	  System.err.println("Usage: generate --metric=NAME --year=YYY --num=NUM_YEARS [--compress] [--millis]");
	  System.err.print(argp.usage());
	  System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
	    ArgP argp = new ArgP();
	    argp.addOption("--metric", "NAME", "Output file name basis");
	    argp.addOption("--year", "YYYY", "starting year");
	    argp.addOption("--num", "NUM_YEARS", "number of years generated");
	    argp.addOption("--compress", "[optional] compress generated file");
	    argp.addOption("--millis", "[optional] use milliseconds");
	    
	    if (args.length < 3) {
	    	usage(argp);
	    }
	    
	    args = CliOptions.parse(argp, args);
	    
	    boolean useCompression = argp.has("--compress");
	    boolean useMillis = argp.has("--millis");
	    String metricName = argp.get("--metric");
	    int startYear = Integer.parseInt(argp.get("--year"));
	    int totalYears = Integer.parseInt(argp.get("--num"));

	    LOG.info("Generating {} years starting from year {}, for metric {}.", totalYears, startYear, metricName);
	    if (useMillis) {
	    	LOG.info("Using MilliSeconds");
	    }
	    if (useCompression) {
	    	LOG.info("Files will be compressed");
	    }
	    
		generateYearlyFiles(useCompression, useMillis, metricName, startYear, totalYears, new Random());
	}
	
	public static void generateYearlyFiles(boolean useCompression, boolean useMillis, String metricName, final int startYear, final int totalYears, final Random rand) throws IOException {
		//TODO add timezone (--tz) param to argp
		Calendar cal = Calendar.getInstance(); // use local timezone
		cal.set(startYear, 0, 1, 0, 0, 0);

		String extension = useCompression ? ".tsd.gz" : ".tsd";
		int sampleSize = useMillis ? 3600000 : 3600;
		long value = 1; // rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;
		long count = 0;

		long startTime = System.currentTimeMillis();

		for (int year = startYear; year < startYear + totalYears; year++) {
			LOG.info("generating year {}", year);

			File metricFolder = new File(""+year);
			metricFolder.mkdirs();
			
			File metricFile = new File(metricFolder, metricName + extension);
			OutputStream os = createOutputStream(useCompression, metricFile);

			long time = useMillis ? cal.getTimeInMillis() : cal.getTimeInMillis() / 1000;
			for (int week = 1; week <= 52; week++) {
				LOG.info("Week {} / 52", week);
				for (int day = 0; day < 7; day++) {
					for (int hour = 0; hour < 24; hour++) {
						for (int i = 0; i < sampleSize; i++) {
							writeRecord(os, metricName, week, day, hour, time, value);
							// Alter the value by a range of +/- RANDOM_GAP
							// value += rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;
							time++;
							count++;
						}
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
	
	private static void writeRecord(OutputStream os, String metricName, int week, int day, int hour, long time, long value) throws IOException {
		String record = metricName + " " + time + " " + value + " " + 
				"week=" + week + " " + "day=" + day /*+ " "+ "hour=" + hour*/ +"\n";
		os.write(record.getBytes());
	}
}
