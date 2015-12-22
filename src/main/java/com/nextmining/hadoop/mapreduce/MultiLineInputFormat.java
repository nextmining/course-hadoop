package com.nextmining.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Reads records that are delimited by a specific begin tag.
 * 
 * @author Younggue Bae
 */
public class MultiLineInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "input.start";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new MultiLineRecordReader();
	}

	/** MultiLineRecordReader */
	public static class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
		private static final Logger LOG = Logger.getLogger(MultiLineRecordReader.class);

		private byte[] startTag;
		private long start;
		private long end;
		private long pos;
		private DataInputStream fsin = null;
		private DataOutputBuffer buffer = new DataOutputBuffer();

		private final LongWritable key = new LongWritable();
		private final Text value = new Text();
		private boolean isEOF = false;

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			if (conf.get(START_TAG_KEY) == null)
				throw new RuntimeException("Error! start tag is unspecified!");

			startTag = conf.get(START_TAG_KEY).getBytes("utf-8");

			FileSplit split = (FileSplit) input;
			start = split.getStart();
			Path file = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec codec = compressionCodecs.getCodec(file);

			FileSystem fs = file.getFileSystem(conf);

			if (codec != null) {
				LOG.info("Reading compressed file " + file + "...");
				fsin = new DataInputStream(codec.createInputStream(fs.open(file)));

				end = Long.MAX_VALUE;
			} else {
				LOG.info("Reading uncompressed file " + file + "...");
				FSDataInputStream fileIn = fs.open(file);

				fileIn.seek(start);
				fsin = fileIn;

				end = start + split.getLength();
			}

			// Because input streams of gzipped files are not seekable, we need to keep track of bytes
			// consumed ourselves.
			pos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (pos < end) {
				try {
					return writeUntilMatch(startTag);
				} finally {
					// Because input streams of gzipped files are not seekable, we need to keep track of
					// bytes consumed ourselves.

					// This is a sanity check to make sure our internal computation of bytes consumed is
					// accurate. This should be removed later for efficiency once we confirm that this code
					// works correctly.

					if (fsin instanceof Seekable) {
						if (pos != ((Seekable) fsin).getPos()) {
							throw new RuntimeException("bytes consumed error!");
						}
					}
				}
			}
			
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return ((float) (pos - start)) / ((float) (end - start));
		}

		private boolean writeUntilMatch(byte[] match) throws IOException {
	
			int i = 0;
			while (!isEOF) {
				int b = fsin.read();
				// increment position (bytes consumed)
				pos++;

				// end of file:
				if (b == -1) {
					//System.out.println("end of file");
					
					key.set(pos);
					value.set(buffer.getData(), 0, buffer.getLength());
					buffer.reset();
					
					isEOF = true;			
					return true;
				}
				
				// save to buffer:
				buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length && pos > startTag.length) {						
						key.set(pos);
						value.set(buffer.getData(), 0, buffer.getLength() - startTag.length);
						buffer.reset();
						buffer.write(startTag);
						
						return true;
					}
					else if (i >= match.length) {
						i = 0;
					}
				} else {
					i = 0;
				}
				// see if we've passed the stop point:
				if (i == 0 && pos >= end) {
					System.out.println("stop point");
					
					return false;
				}
			}
			
			return false;
		}
	}
}