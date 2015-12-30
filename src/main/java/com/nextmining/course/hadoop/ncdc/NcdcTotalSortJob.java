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
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(TotalOrderPartitioner.class);

        Path partitionFile = new Path(outputPath, "_partition");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);

        double uniformProbability = 0.1;
        int maximumNumberOfSamples = 5000;
        int maximumNumberOfSplits = 5;
        InputSampler.Sampler<IntWritable, Text> sampler =
                new InputSampler.RandomSampler<IntWritable, Text>(uniformProbability, maximumNumberOfSamples, maximumNumberOfSplits);

        InputSampler.writePartitionFile(job, sampler);

        // Add to DistributedCache
        /*
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);
        */

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
