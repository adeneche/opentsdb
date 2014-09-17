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
			System.err.println("usage speedtest [buffer BUFFER-SIZE] [ (channel | channel_direct) BUFFER-SIZE [NUM-LINES]] [ (channel2 | channel2_direct) BUFFER-SIZE [BYTES-SIZE] NUM-LINES] path");
			System.exit(-1);
		}
		
		if (args[0].equals("buffer")) {
			readBuffer(args);
		} else if (args[0].equals("channel")) {
			readChannel(args, false);
		} else if (args[0].equals("channel_direct")) {
			readChannel(args, true);
		} else if (args[0].equals("channel2")){
			readChannel2(args, false);
		} else if (args[0].equals("channel2_direct")) {
			readChannel2(args, true);
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
	
	private static void readChannel(final String[] args, final boolean direct) throws IOException {
		System.out.println("using FileChannel with charset decoding...");
		if (direct) System.out.println("using allocateDirect...");

		if (args.length < 3) return;
		
		int curArg = 1;
		final int bufferSize = Integer.parseInt(args[curArg++]); // in Koctets
		if (bufferSize <= 0) return;
		
		final int numLines = (args.length > 3) ? Integer.parseInt(args[curArg++]) : 0;
		if (numLines < 0) return;
		
		final String path = args[curArg++];

		final FileInputStream fis = new FileInputStream(path);
		final FileChannel fc = fis.getChannel();
		//TODO compare between allocate and allocateDirect
		final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufferSize) : ByteBuffer.allocate(1024*bufferSize);
		Charset encoding = Charset.defaultCharset();
		
		final char[] line = new char[1024]; 
		int nextChar = 0;
		int numChars = 0;
		boolean skipLF = false;
		int points = 0;

		final long start_time = System.nanoTime();

		int total = 0;
		
		while (fc.read(bb) > 0) {
			bb.flip();
			
			total += bb.limit();
			
			final CharBuffer cb = encoding.decode(bb);
			numChars = cb.limit();
			
			if (numLines == 0) {
				
				for (int i = 0; i < numChars; i++) {
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
		
		System.out.println("Total Bytes read: "+total);
		displayAvgSpeed(start_time, points);

		fc.close();
		fis.close();
	}
	
	private static void readChannel2(final String[] args, final boolean direct) throws IOException {
		System.out.println("using FileChannel without charset decoding...");
		if (direct) System.out.println("using allocateDirect...");
		
		if (args.length < 4) return;
		
		int curArg = 1;
		
		final int bufferSize = Integer.parseInt(args[curArg++]); // in Koctets
		if (bufferSize <= 0) return;
		
		final int numBytes = args.length < 5 ? bufferSize : Integer.parseInt(args[curArg++]);
		
		final int numLines = Integer.parseInt(args[curArg++]);
		if (numLines < 0) return;
		
		final String path = args[curArg++];

		final FileInputStream fis = new FileInputStream(path);
		final FileChannel fc = fis.getChannel();
		final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufferSize) : ByteBuffer.allocate(1024*bufferSize);
		final byte[] bytes = new byte[1024*numBytes];
		//TODO try a different size for the bytes array
		
		final long start_time = System.nanoTime();
		int total = 0;
		
		while (fc.read(bb) > 0) {
			bb.flip();
			
			int offset = 0;
			int avail = bb.limit();
			
			while (offset < avail) {
				if ((offset + bytes.length) > avail) {
					bb.get(bytes, 0, avail - offset);
					total += avail - offset;
					offset = avail;
				}
				else {
					bb.get(bytes);
					total += bytes.length;
					offset += bytes.length;
				}
			}

			bb.clear();
		}
		
		System.out.println("Total Bytes read: "+total);
		displayAvgSpeed(start_time, numLines);

		fc.close();
		fis.close();
	}

	private static void displayAvgSpeed(final long start_time, final int points) {
		final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
		LOG.info(String.format("Average speed: %d data points in %.3fs (%.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

}
