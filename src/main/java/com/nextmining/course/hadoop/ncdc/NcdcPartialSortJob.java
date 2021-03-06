package com.nextmining.course.hadoop.ncdc;

import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Partial sorting job.
 **
 * @author Younggue Bae
 */
public class NcdcPartialSortJob extends AbstractJob {

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcPartialSortJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("input", "i", "The path for job input(comma separated)", true);
        addOption("output", "o", "The path for job output", true);

        parseArguments(args);

        // input path
        String[] inputs = getOption("input").split(",");
        Set<Path> inputPaths = new HashSet<Path>();
        for (String input : inputs) {
            inputPaths.add(new Path(input));
        }

        // output path
        Path outputPath = new Path(getOption("output"));

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME_PREFIX + getClass().getSimpleName());
        job.setJarByClass(NcdcPartialSortJob.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(NcdcPartialSortMapper.class);
        job.setReducerClass(NcdcPartialSortReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setSortComparatorClass(KeyComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Comparator for sorting values by descending.
     */
    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(IntWritable.class, true);
        }

        /**
         * Compares in the descending order of the keys.
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable o1 = (IntWritable) a;
            IntWritable o2 = (IntWritable) b;
            if (o1.get() < o2.get()) {
                return 1;
            } else if(o1.get() > o2.get()) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * Mapper.
     */
    public static class NcdcPartialSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.parse(value);

            if (parser.isValidTemperature()) {
                String year = parser.getYear();
                int airTemperature = parser.getAirTemperature();

                context.write(new IntWritable(airTemperature), new Text(year + "\t" + airTemperature));
            }
            else if (parser.isMalformedTemperature()) {
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            }
            else if (parser.isMissingTemperature()) {
                context.getCounter(Temperature.MISSING).increment(1);
            }
        }
    }

    /**
     * Reducer.
     */
    public static class NcdcPartialSortReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }
}
