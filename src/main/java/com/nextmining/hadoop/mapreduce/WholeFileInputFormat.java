package com.nextmining.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Reads records that are whole file such as json input file.
 * 
 * @author Younggue Bae
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit,
																		TaskAttemptContext context) throws IOException, InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(inputSplit, context);
		return reader;
	}

	/** WhileFileRecordReader */
	public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

		private FileSplit split;
		private Configuration conf;

		private final BytesWritable value = new BytesWritable();
		private boolean processed = false;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
				InterruptedException {
			this.split = (FileSplit) split;
			this.conf = context.getConfiguration();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (processed) {
				return false;
			}

			int fileLength = (int) split.getLength();
			byte[] result = new byte[fileLength];

			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);
			
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, result, 0, fileLength);
				value.set(result, 0, fileLength);

			} finally {
				IOUtils.closeStream(in);
			}
			this.processed = true;
			return true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
			// nothing to close
		}
	}

}