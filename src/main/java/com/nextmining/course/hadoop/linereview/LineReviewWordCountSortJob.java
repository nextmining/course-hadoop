package com.nextmining.course.hadoop.linereview;

import com.nextmining.hadoop.io.IntPairWritable;
import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Secondary sort job for LINE app store review analysis result.
 *
 * @author Younggue Bae
 */
public class LineReviewWordCountSortJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(LineReviewWordCountSortJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LineReviewWordCountSortJob(), args);
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
        job.setJarByClass(LineReviewWordCountSortJob.class);
        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(LineReviewWordCountSortMapper.class);
        job.setReducerClass(LineReviewWordCountSortReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);    // Partitioner
        job.setSortComparatorClass(KeyComparator.class);    // Sort comparator
        job.setGroupingComparatorClass(GroupComparator.class);  // Group comparator
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, outputPath);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Partitioner.
     */
    public static class KeyPartitioner
            extends Partitioner<IntPairWritable, Text> {

        @Override
        public int getPartition(IntPairWritable key, Text value, int numPartitions) {
            // multiply by 127 to perform some mixing
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    /**
     * KeyComparator
     */
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPairWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPairWritable ip1 = (IntPairWritable) w1;
            IntPairWritable ip2 = (IntPairWritable) w2;
            int cmp = IntPairWritable.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return -IntPairWritable.compare(ip1.getSecond(), ip2.getSecond()); //descending
        }
    }

    /**
     * GroupComparator.
     */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPairWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPairWritable ip1 = (IntPairWritable) w1;
            IntPairWritable ip2 = (IntPairWritable) w2;
            return IntPairWritable.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    /**
     * Mapper.
     */
    public static class LineReviewWordCountSortMapper
            extends Mapper<LongWritable, Text, IntPairWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] field = record.split("\t");
            int rating = Integer.parseInt(field[0]);
            String word = field[1];
            int count = Integer.parseInt(field[2]);

            context.write(new IntPairWritable(rating, count),
                    new Text(rating + "\t" + word + "\t" + count));
        }
    }

    /**
     * Reducer.
     */
    public static class LineReviewWordCountSortReducer
            extends Reducer<IntPairWritable, Text, Text, NullWritable> {

        @Override
        protected void reduce(IntPairWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }

}
