package com.nextmining.course.hadoop.ncdc;

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
 * Secondary sorting job.
 *
 * @author Younggue Bae
 */
public class NcdcSecondarySortJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcSecondarySortJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcSecondarySortJob(), args);
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
        job.setJarByClass(NcdcTotalSortJob.class);
        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(NcdcSecondarySortMapper.class);
        job.setReducerClass(NcdcSecondarySortReducer.class);
        job.setPartitionerClass(FirstPartitioner.class);    // Partitioner
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
    public static class FirstPartitioner
            extends Partitioner<IntPairWritable, NullWritable> {

        @Override
        public int getPartition(IntPairWritable key, NullWritable value, int numPartitions) {
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
    public static class NcdcSecondarySortMapper
            extends Mapper<LongWritable, Text, IntPairWritable, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntPairWritable(parser.getYearInt(), parser.getAirTemperature()),
                        new Text(parser.getYearInt() + "\t" + parser.getAirTemperature()));
            }
        }
    }

    /**
     * Reducer.
     */
    public static class NcdcSecondarySortReducer
            extends Reducer<IntPairWritable, Text, Text, NullWritable> {

        @Override
        protected void reduce(IntPairWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /**
             * For max temperature by year
             */
            //context.write(new Text(key.getFirst() + "\t" + key.getSecond()), NullWritable.get());

            for (Text val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }

}
