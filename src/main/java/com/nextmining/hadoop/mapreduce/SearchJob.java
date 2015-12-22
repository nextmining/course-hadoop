package com.nextmining.hadoop.mapreduce;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


/**
 * This class is a driver to search lines by key.
 * 
 * @author Younggue Bae
 */
public class SearchJob extends AbstractJob {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractJob.class);
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SearchJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		addOption("input", "i", "Path to job input directory(comma separated)", true);
		addOption("output", "o", "The directory pathname for output", true);
		addOption("delimiter", null, "The delimiter", true);
		addOption("columns", null, "The column indexs to find(comma separated)", true);
		addOption("search", null, "The search keyword to find(comma separated)", true);
		
		parseArguments(args);

		String inputs = getOption("input");
		String[] input = inputs.split(",");
    String output = getOption("output");
    
    String delimiter = getOption("delimiter");
    if (delimiter.equals("\\t")) {
    	delimiter = "\t";
    }

		Path[] inputPath = new Path[input.length];
		for (int i = 0; i < input.length; i++) {
			inputPath[i] = new Path(input[i].trim());
		}
		Path outputPath = new Path(output);
	  
		Configuration conf = getConf();
		conf.set("delimiter", delimiter);
		conf.set("columns", getOption("columns"));
		conf.set("search", getOption("search"));
    
		//Job job = new Job(conf);
		Job job = Job.getInstance(conf);
    
    logger.info("search == " + getOption("search"));
    
    job.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), job, SearchMapper.class, Reducer.class));
    job.setJarByClass(SearchJob.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(SearchMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setReducerClass(SearchReducer.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    HadoopUtil.delete(conf, outputPath);
    
    job.waitForCompletion(true);
		
		return 0;
	}
	
	/** SearchMapper */
	public static class SearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String delimiter;
		private List<String> search;
		private int[] columns;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			
			delimiter = conf.get("delimiter");
			search = Arrays.asList(conf.get("search").split(","));
			
			String[] strColumns = conf.get("columns").split(",");
			columns = new int[strColumns.length];
			for (int i = 0; i < strColumns.length; i++) {
				columns[i] = Integer.parseInt(strColumns[i]);
			}
			
			System.out.println("search == " + search);
			System.out.println("columns == " + conf.get("columns"));
			
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] field = line.split(delimiter);
			
			boolean match = false;
			
			for (int i = 0; i < columns.length; i++) {
				String searchField = field[columns[i]];
				
				if (search.contains(searchField)) {
					match = true;
					break;
				}
			}
			
			if (match) {
				context.write(new Text(line), NullWritable.get());
			}
		}
	}
	
	/** SearchReducer */
	public static class SearchReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

}
