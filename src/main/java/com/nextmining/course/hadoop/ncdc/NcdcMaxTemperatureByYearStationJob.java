package com.nextmining.course.hadoop.ncdc;

import com.nextmining.hadoop.io.TextPairWritable;
import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Max temperature by year and station job.
 *
 * @author Younggue Bae
 */
public class NcdcMaxTemperatureByYearStationJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcMaxTemperatureByYearStationJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcMaxTemperatureByYearStationJob(), args);
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
        job.setJarByClass(NcdcMaxTemperatureByYearStationJob.class);
        job.setMapOutputKeyClass(TextPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(NcdcMaxTemperatureMapper.class);
        job.setReducerClass(NcdcMaxTemperatureReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);    // Partitioner
        job.setGroupingComparatorClass(TextPairWritable.FirstComparator.class);  // Group comparator
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
            extends Partitioner<TextPairWritable, IntWritable> {

        @Override
        public int getPartition(TextPairWritable key, IntWritable value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * Mapper.
     */
    public static class NcdcMaxTemperatureMapper
            extends Mapper<LongWritable, Text, TextPairWritable, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new TextPairWritable(parser.getYear(), parser.getStationId()),
                        new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * Reducer.
     */
    public static class NcdcMaxTemperatureReducer
            extends Reducer<TextPairWritable, IntWritable, Text, IntWritable> {

        private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
        private Map<String, String> stationNames = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Configuration conf = context.getConfiguration();

            this.stationNames = loadStationNames("stations-fixed-width.txt");
        }

        private Map<String, String> loadStationNames(String stationFile) throws IOException {
            Map<String, String> result = new HashMap<String, String>();

            BufferedReader in =
                    new BufferedReader(new InputStreamReader(new FileInputStream(stationFile), "utf-8"));

            System.err.println("Station:");
            String line;
            while ((line = in.readLine()) != null) {
                // right trim
                line = line.replaceAll("\\s+$", "");
                if (!line.equals("")) {
                    parser.parse(line);
                    String stationId = parser.getStationId();
                    String stationName = parser.getStationName();
                    System.err.println(stationId + "\t" + stationName);
                    result.put(stationId, stationName);
                }
            }

            in.close();

            return result;
        }

        @Override
        protected void reduce(TextPairWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String year = key.getFirst().toString();
            String stationId = new String(key.getSecond().toString());
            String stationName = stationNames.get(stationId);

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                //String stationId = key.getSecond().toString();
                //String stationName = stationNames.get(stationId);

                maxValue = Math.max(maxValue, val.get());
                context.write(new Text(year + "\t" + stationId + "\t" + stationName), val);
            }
            //context.write(new Text(year + "\t" + stationId + "\t" + stationName),
            //        new IntWritable(maxValue));
        }
    }

}
