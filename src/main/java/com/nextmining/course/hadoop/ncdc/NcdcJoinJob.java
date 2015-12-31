package com.nextmining.course.hadoop.ncdc;

import com.nextmining.hadoop.io.IntPairWritable;
import com.nextmining.hadoop.io.TextPairWritable;
import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Join job.
 *
 * @author Younggue Bae
 */
public class NcdcJoinJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcJoinJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcJoinJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("inputNcdc", null, "The path for job input(ncdc)", true);
        addOption("inputStation", null, "The path for job input(station)", true);
        addOption("output", "o", "The path for job output", true);

        parseArguments(args);

        // input path
        Path inputNcdcPath = new Path(getOption("inputNcdc"));
        Path inputStationPath = new Path(getOption("inputStation"));

        // output path
        Path outputPath = new Path(getOption("output"));

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME_PREFIX + getClass().getSimpleName());
        job.setJarByClass(NcdcJoinJob.class);
        job.setMapOutputKeyClass(TextPairWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(NcdcJoinReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);    // Partitioner
        job.setGroupingComparatorClass(TextPairWritable.FirstComparator.class);  // Group comparator
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, inputNcdcPath,
                TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, inputStationPath,
                TextInputFormat.class, JoinStationMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Partitioner.
     */
    public static class KeyPartitioner extends Partitioner<TextPairWritable, Text> {
        @Override
        public int getPartition(TextPairWritable key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * JoinRecordMapper.
     */
    public static class JoinRecordMapper extends Mapper<LongWritable, Text, TextPairWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new TextPairWritable(parser.getStationId(), "1"), value);
        }
    }

    /**
     * JoinStationMapper
     */
    public static class JoinStationMapper extends Mapper<LongWritable, Text, TextPairWritable, Text> {
        private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (parser.parse(value)) {
                context.write(new TextPairWritable(parser.getStationId(), "0"),
                        new Text(parser.getStationName()));
            }
        }
    }

    /**
     * NcdcJoinReducer.
     */
    public static class NcdcJoinReducer extends Reducer<TextPairWritable, Text, Text, Text> {

        @Override
        protected void reduce(TextPairWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            Text stationName = new Text(iter.next());
            while (iter.hasNext()) {
                Text record = iter.next();
                Text outValue = new Text(stationName.toString() + "\t" + record.toString());
                context.write(key.getFirst(), outValue);
            }
        }
    }

}
