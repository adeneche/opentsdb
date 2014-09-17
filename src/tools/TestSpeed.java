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

/**
 * Simple tool to test how fast various strategies for loading files into memory.
 * - BufferedReader + readLine()
 * - FileChannel + ByteBuffer
 * - ...
 */
public class TestSpeed {

	private static final Logger LOG = LoggerFactory.getLogger(TestSpeed.class);

	public static void main(String[] args) throws IOException {
		
		if (args.length == 0) {
			System.err.println("usage speedtest [buffer BUFFER-SIZE] [channel BUFFER-SIZE [NUM-LINES]] path");
			System.exit(-1);
		}
		
		if (args[0].equals("buffer")) {
			readBuffer(args);
			
		} else if (args[0].equals("channel")) {
			readChannel(args);
			
		} else {
			
		}

	}
	
	private static void readBuffer(final String[] args) throws IOException {
		System.out.println("using BufferedReader...");
		
		if (args.length < 3) return;
		
		final int bufferSize = Integer.parseInt(args[1]); // in Koctets
		if (bufferSize <= 0) return;
		
		final String path = args[2];
		
		InputStream is = new FileInputStream(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(is), bufferSize*1024);
		
		String line = null;
		int numLines = 0;
		final long start_time = System.nanoTime();
		
		while ((line = in.readLine()) != null) {
			numLines++;
		}

		displayAvgSpeed(start_time, numLines);

		in.close();
	}
	
	private static void readChannel(final String[] args) throws IOException {
		System.out.println("using FileChannel...");
		
		if (args.length < 3) return;
		
		final int bufferSize = Integer.parseInt(args[1]); // in Koctets
		if (bufferSize <= 0) return;
		
		final int numLines = (args.length > 3) ? Integer.parseInt(args[2]) : 0;
		if (numLines < 0) return;
		
		final String path = (args.length > 3) ? args[3] : args[2];

		final FileInputStream fis = new FileInputStream(path);
		final FileChannel fc = fis.getChannel();
		final ByteBuffer bb = ByteBuffer.allocate(1024*bufferSize);
		Charset encoding = Charset.defaultCharset();

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
