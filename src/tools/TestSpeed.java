package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSpeed {

	private static final Logger LOG = LoggerFactory.getLogger(TestSpeed.class);

	private static void usage(final ArgP argp) {
		System.err.println("Usage: testspeed [--bb=SIZE] [--buffer=SIZE] [--lines] path");
		System.err.print(argp.usage());
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		ArgP argp = new ArgP();
		argp.addOption("--bb", "use byte buffer. SIZE is the number of lines in the file");
		argp.addOption("--buffer", "SIZE", "(default 64) size of ByteBuffer in Koctets");
		
		args = CliOptions.parse(argp, args);
		if (args == null || args.length < 1) {
			usage(argp);
		}
		
		final String path = args[0];
		final boolean useByteBuffer = argp.has("--bb");
		
		if (useByteBuffer) {
			final int numLines = argp.get("--bb") != null ? Integer.parseInt(argp.get("--bb")) : 0;
			final int bufferSize = Integer.parseInt(argp.get("--buffer", "64"));
			readWithBB(path, numLines, bufferSize);
		} else {
			readWithBR(path);
		}
	}
	
	private static void readWithBR(final String path) throws IOException {
		InputStream is = new FileInputStream(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		
		String line = null;
		int numLines = 0;
		final long start_time = System.nanoTime();
		
		while ((line = in.readLine()) != null) {
			numLines++;
		}

		displayAvgSpeed(start_time, numLines);

		in.close();
	}
	
	private static void readWithBB(final String path, final int numLines, final int bufferSize) throws IOException {
		final FileInputStream fis = new FileInputStream(path);
		final FileChannel fc = fis.getChannel();
		final ByteBuffer bb = ByteBuffer.allocate(1024*bufferSize);
		Charset encoding = Charset.forName(System.getProperty("file.encoding"));

		final char[] line = new char[1024]; 
		int nextChar = 0;
		boolean skipLF = false;
		int points = 0;

		final long start_time = System.nanoTime();

		while (fc.read(bb) > 0) {
			bb.flip();
			
			final CharBuffer cb = encoding.decode(bb);

			if (numLines == 0) {
				for (int i = 0; i < cb.limit(); i++) {
					char c = cb.get();

					if (c == '\n' && skipLF) { // ignore this '\n'
						skipLF = false;
					} else if (c == '\n' || c == '\r') {
						// handle line[0..nextChar[
						nextChar = 0;
						points++;

						if (c == '\r') {
							skipLF = true;
						}
					} else {
						line[nextChar++] = c;
					}
				}
			}

			bb.clear();
		}

		if (numLines > 0) points = numLines;
		
		displayAvgSpeed(start_time, points);

		fc.close();
		fis.close();
	}

	private static void displayAvgSpeed(final long start_time, final int points) {
		final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
		LOG.info(String.format("Average speed: %d data points in %.3fs (%.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

}
