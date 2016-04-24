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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Total sort job.
 *
 * 참조:
 * https://pipiper.wordpress.com/2013/05/02/sorting-using-hadoop-totalorderpartitioner/
 * https://github.com/tomwhite/hadoop-book/blob/master/ch09-mr-features/src/main/java/SortByTemperatureUsingTotalOrderPartitioner.java
 *
 * @author Younggue Bae
 */
public class NcdcTotalSortJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcTotalSortJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcTotalSortJob(), args);
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
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(NcdcTotalSortMapper.class);
        job.setReducerClass(NcdcTotalSortReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);

        Path inputDir = new Path("/user/student/ygbae/ncdc");
        Path partitionFilePath = new Path(inputDir, "_partition");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFilePath);

        double uniformProbability = 0.1;
        int maximumNumberOfSamples = 10000;
        int maximumNumberOfSplits = 10;
        InputSampler.Sampler<IntWritable, Text> sampler =
                new InputSampler.RandomSampler<IntWritable, Text>(uniformProbability, maximumNumberOfSamples, maximumNumberOfSplits);

        InputSampler.writePartitionFile(job, sampler);

        // Add to DistributedCache
        /**
         * [주의] getPartitionFile(conf)와 같이 사용할 경우 아래와 같은 에러 발생하므로
         * getPartitionFile(job.getConfiguration()와 같이 해주어야 정상적으로 실행된다
         * Exception in thread "main" java.io.FileNotFoundException: File does not exist: hdfs://nextmining04:8020/user/student/_partition.lst
         * 참조: http://m.blog.csdn.net/article/details?id=25649945
         */
        //String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
        logger.info("*** partitionFile = " + partitionFile);
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Comparator for sorting values by descending.
     */
    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(LongWritable.class, true);
        }

        /**
         * Compares in the descending order of the keys.
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            LongWritable o1 = (LongWritable) a;
            LongWritable o2 = (LongWritable) b;
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
    public static class NcdcTotalSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.parse(value);

            if (parser.isValidTemperature()) {
                String year = parser.getYear();
                int airTemperature = parser.getAirTemperature();

                context.write(new LongWritable(airTemperature), new Text(year + "\t" + airTemperature));
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
    public static class NcdcTotalSortReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }
}
