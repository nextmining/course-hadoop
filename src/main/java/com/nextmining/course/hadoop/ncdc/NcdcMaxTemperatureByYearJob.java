package com.nextmining.course.hadoop.ncdc;

import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

/**
 * Max temperature job.
 *
 * @author Younggue Bae
 */
public class NcdcMaxTemperatureByYearJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcMaxTemperatureByYearJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcMaxTemperatureByYearJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("input", "i", "The path for job input(ncdc)", true);
        addOption("output", "o", "The path for job output", true);

        parseArguments(args);

        // input path
        Path inputPath = new Path(getOption("input"));

        // output path
        Path outputPath = new Path(getOption("output"));

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME_PREFIX + getClass().getSimpleName());
        job.setJarByClass(NcdcMaxTemperatureByYearJob.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(NcdcMaxTemperatureMapper.class);
        job.setReducerClass(NcdcMaxTemperatureReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper.
     */
    public static class NcdcMaxTemperatureMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * Reducer.
     */
    public static class NcdcMaxTemperatureReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new IntWritable(maxValue));
        }
    }

}
