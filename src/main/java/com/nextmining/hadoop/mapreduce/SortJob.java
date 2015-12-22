package com.nextmining.hadoop.mapreduce;

import com.nextmining.hadoop.io.Sortable;
import com.nextmining.hadoop.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * This class is a driver to sort the column with simple integer value.
 * 
 * @author Younggue Bae
 */
public class SortJob extends AbstractJob {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SortJob(), args);
	}
	
	@SuppressWarnings("unused")
	@Override
	public int run(String[] args) throws Exception {
		addOption("input", "i", "Path to job input directory.", true);
		addOption("output", "o", "The directory pathname for output.", true);
		addOption("sortColumn", null, "The sort column index.", true);
		addOption("sortOption", null, "The sort option.(asc or desc, The default is asc)", false);
		addOption("sortDatatype", null, "The sort column datatype.(ex. string, numeric)", false);
		addOption("delimiter", "d", "Delimiter(The default is \\t)", false);

		parseArguments(args);

		String inputs = getOption("input");
		String[] input = inputs.split(",");
		String output = getOption("output");
		String sortColumnIndex = getOption("sortColumn");
		String sortOption = getOption("sortOption", "asc");
		String sortDatatype = getOption("sortDatatype", "numeric");
		String delimiter = getOption("delimiter");
		
    if (delimiter.equals("\\t")) {
    	delimiter = "\t";
    }

		Configuration conf = getConf();

		conf.setInt("sortColumn", Integer.parseInt(sortColumnIndex));
		conf.set("sortOption", sortOption);
		conf.set("sortDatatype", sortDatatype);
		conf.set("delimiter", delimiter);

		logger.info("sortColumn == " + sortColumnIndex);
		logger.info("sortOption == " + sortOption);
		logger.info("sortDatatype == " + sortDatatype);
		logger.info("delimiter == " + delimiter);

		Path[] inputPath = new Path[input.length];
		for (int i = 0; i < input.length; i++) {
			inputPath[i] = new Path(input[i].trim());
		}
		Path outputPath = new Path(output);

		//Job job = new Job(conf);
		Job job = Job.getInstance(conf);

		String srtInputs = inputs;
		if (inputs.length() > 30) {
			srtInputs = "..." + inputs.substring(inputs.length() - 30);
		}
		//job.setJobName("Sort" + "(" + srtInputs + ")");
		job.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), job, SortMapper.class, Reducer.class));
		job.setJarByClass(SortJob.class);
		job.setMapOutputKeyClass(Sortable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(SortMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		HadoopUtil.delete(conf, outputPath);

		job.waitForCompletion(true);

		return 0;
	}
	
	public static class SortMapper extends Mapper<LongWritable, Text, Sortable, Text> {

		private String delimiter = "\t";
		private String sortOption = "ascending";
		private String sortDatatype;
		private int sortColumnIndex;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();

			delimiter = conf.get("delimiter", "\t");

			String option = conf.get("sortOption", "ascending").toLowerCase();
			if (option.startsWith("asc")) {
				sortOption = "ascending";
			} else if (option.startsWith("desc")) {
				sortOption = "descending";
			}

			System.out.println("sort option == " + option);

			sortColumnIndex = conf.getInt("sortColumn", -1);
			sortDatatype = conf.get("sortDatatype");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] field = record.split(delimiter);

			try {
				String strVal = field[sortColumnIndex];
				
				Sortable sortValue = new Sortable(sortOption, sortDatatype, strVal);
				context.write(sortValue, new Text(record));
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(e.getMessage());
				System.err.println("error: record == " + record);
				System.err.println("errot: sort column == " + field[sortColumnIndex]);
				throw new InterruptedException(e.getMessage());
			}
		}
	}

	public static class SortReducer extends Reducer<Sortable, Text, Text, NullWritable> {

		@Override
		protected void reduce(Sortable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text val : values) {
				context.write(val, NullWritable.get());
			}
		}

	}

}
