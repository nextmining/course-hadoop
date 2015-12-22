package com.nextmining.hadoop.mapreduce;


import com.nextmining.hadoop.io.TextPairWritable;
import com.nextmining.hadoop.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


/**
 * This class is a driver to join the base input with joinable input.
 * 
 * @author Younggue Bae
 */
public class JoinJob extends AbstractJob {

	@SuppressWarnings("unused")
	@Override
	public int run(String[] args) throws Exception {
		addOption("inputBase", "i", "Base input path to join.", true);
		addOption("inputJoin", "j", "Input path to join with main input.", true);
		addOption("output", "o", "The directory pathname for output.", true);
		addOption("baseKeyColumns", null, "The Key column indexes of base input.(comma delimiter)", true);
		addOption("joinKeyColumns", null, "The Key column indexes of join input.(comma delimiter)", true);
		addOption("joinValueColumns", null, "The value column indexes of join input.(comma delimiter)", true);
		addOption("delimiter", "d", "Delimiter(The default is \\t)", false);

		parseArguments(args);

		String inputBase = getOption("inputBase");
		String inputJoin = getOption("inputJoin");
		String output = getOption("output");
		String[] baseKeyColumns = getOption("baseKeyColumns").split(",");
		String[] joinKeyColumns = getOption("joinKeyColumns").split(",");
		String[] joinValueColumns = getOption("joinValueColumns").split(",");
		String delimiter = getOption("delimiter");
		
    if (delimiter.equals("\\t")) {
    	delimiter = "\t";
    }

		Configuration conf = getConf();

		Path inputBasePath = new Path(inputBase);
		Path inputJoinPath = new Path(inputJoin);
		Path outputPath = new Path(output);

		conf.setStrings("baseKeyColumns", baseKeyColumns);
		conf.setStrings("joinKeyColumns", joinKeyColumns);
		conf.setStrings("joinValueColumns", joinValueColumns);
		conf.setInt("joinValueColumnSize", joinValueColumns.length);
		conf.set("inputBaseFile", inputBasePath.toString());
		conf.set("inputJoinFile", inputJoinPath.toString());
		conf.set("delimiter", delimiter);

		//Job job = new Job(conf);
		Job job = Job.getInstance(conf);

		String srtInputBase = inputBase;
		if (inputBase.length() > 30) {
			srtInputBase = "..." + inputBase.substring(inputBase.length() - 30);
		}
		String srtInputJoin = inputJoin;
		if (inputJoin.length() > 30) {
			srtInputJoin = "..." + inputJoin.substring(inputJoin.length() - 30);
		}
		//job.setJobName("Join on " + "(" + srtInputBase + ") and (" + srtInputJoin + ")");
		job.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), job, JoinMapper.class, Reducer.class));
		job.setJarByClass(JoinJob.class);
		job.setMapOutputKeyClass(TextPairWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(JoinMapper.class);
		job.setPartitionerClass(JoinKeyPartitioner.class);
		job.setGroupingComparatorClass(TextPairWritable.FirstComparator.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputBasePath);
		FileInputFormat.addInputPath(job, inputJoinPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		HadoopUtil.delete(conf, outputPath);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinJob(), args);
	}

	/**
	 * This class is a mapper to join.
	 */
	static class JoinMapper extends Mapper<LongWritable, Text, TextPairWritable, Text> {

		private String delimiter = "\t";
		private String inputTag;
		private String[] baseKeyColumns;
		private String[] joinKeyColumns;
		private String[] joinValueColumns;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String currentInputFile = fileSplit.getPath().toString();

			delimiter = conf.get("delimiter", "\t");

			String inputBaseFile = conf.get("inputBaseFile");
			String inputJoinFile = conf.get("inputJoinFile");
			baseKeyColumns = conf.getStrings("baseKeyColumns");
			joinKeyColumns = conf.getStrings("joinKeyColumns");
			joinValueColumns = conf.getStrings("joinValueColumns");

			System.out.println("currentInputFile == " + currentInputFile);
			System.out.println("inputBaseFile == " + inputBaseFile);
			System.out.println("inputJoinFile == " + inputJoinFile);
			System.out.println("baseKeyColumns == " + Arrays.asList(baseKeyColumns));
			System.out.println("joinKeyColumns == " + Arrays.asList(joinKeyColumns));
			System.out.println("joinValueColumns == " + Arrays.asList(joinValueColumns));

			if (currentInputFile.indexOf(inputBaseFile) >= 0) {
				inputTag = "BASE";
			} else if (currentInputFile.indexOf(inputJoinFile) >= 0) {
				inputTag = "JOIN";
			} else {
				throw new IOException("Failed in identifying join input tag from input files!");
			}
		}

		private String makeKey(String record) {
			StringBuffer key = new StringBuffer();

			String[] field = record.split(delimiter);
			if (inputTag.equals("BASE")) {
				for (int i = 0; i < baseKeyColumns.length; i++) {
					String column = baseKeyColumns[i];
					int index = Integer.parseInt(column);
					if (i < baseKeyColumns.length - 1) {
						key.append(field[index]).append(delimiter);
					}
					else {
						key.append(field[index]);
					}
				}
			} else if (inputTag.equals("JOIN")) {
				for (int i = 0; i < joinKeyColumns.length; i++) {
					String column = joinKeyColumns[i];
					int index = Integer.parseInt(column);
					if (i < joinKeyColumns.length - 1) {
						key.append(field[index]).append(delimiter);
					}
					else {
						key.append(field[index]);
					}
				}
			}

			return key.toString();
		}

		private String makeValue(String record) {
			StringBuffer value = new StringBuffer();

			String[] field = record.split(delimiter);

			if (inputTag.equals("BASE")) {
				value.append(record);
			} else if (inputTag.equals("JOIN")) {
				for (int i = 0; i < joinValueColumns.length; i++) {
					String column = joinValueColumns[i];
					int index = Integer.parseInt(column);
					
					if (i < joinValueColumns.length - 1) {
						value.append(field[index]).append(delimiter);
					}
					else {
						value.append(field[index]);
					}
				}
			}

			return value.toString();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();

			String strKey = makeKey(record);
			String strValue = makeValue(record);
			
			int order = 0;
			if (inputTag.equals("JOIN")) {
				order = 1;
				
				//System.out.println("join == " + strValue);
			}
			else if (inputTag.equals("BASE")) {
				order = 2;
			}
			context.write(new TextPairWritable(strKey, order + "_" + inputTag), new Text(strValue));
		}
	}

	/**
	 * This class is a reducer to join.
	 */
	static class JoinReducer extends Reducer<TextPairWritable, Text, NullWritable, Text> {

		private String delimiter = "\t";
		private String emptyJoinValue;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();

			delimiter = conf.get("delimiter", "\t");
			int joinValueColumnSize = conf.getInt("joinValueColumnSize", 0);
			
			StringBuffer sbEmptyJoinValue = new StringBuffer();
			for (int i = 0; i < joinValueColumnSize; i++) {
				if (i < joinValueColumnSize - 1) {
					sbEmptyJoinValue.append(" ").append(delimiter);
				}
				else {
					sbEmptyJoinValue.append(" ");
				}
			}
			emptyJoinValue = sbEmptyJoinValue.toString();
			
			System.out.println("joinValueColumnSize == " + joinValueColumnSize);
		}
		
		@Override
		protected void reduce(TextPairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> baseValueList = new ArrayList<String>();
			Set<String> joinValueList = new HashSet<String>();
			
			for (Text val : values) {
				String inputTag = key.getSecond().toString();
				
				String line = val.toString();
				
				// identify the record source
				if (inputTag.equals("1_JOIN")) {
					joinValueList.add(line);
					//System.out.println("join == " + key.toString() + " => " + line);
				} else if (inputTag.equals("2_BASE")) {
					baseValueList.add(line);
					//System.out.println("base == " + key.toString() + " => " + line);
				}
			}

			// pump final output to file
			for (String baseValue : baseValueList) {
				if (joinValueList.size() > 0) {
					// for outer join
					for (String joinValue : joinValueList) {
						context.write(NullWritable.get(), new Text(baseValue + delimiter + joinValue));
					}
				}
				else {
					context.write(NullWritable.get(), new Text(baseValue + delimiter + emptyJoinValue));
				}
			}
		}
	}
	
	/**
	 * This class is a partitioner.
	 */
	static class JoinKeyPartitioner extends Partitioner<TextPairWritable, Text> {

		@Override
		public int getPartition(TextPairWritable key, Text value, int numPartitions) {
			Text strKey = key.getFirst();

			return (strKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}	
	}
	
}
