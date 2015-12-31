package com.nextmining.course.hadoop.ncdc;

import com.nextmining.hadoop.io.TextPairWritable;
import com.nextmining.hadoop.mapreduce.AbstractJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Add station name into record by using side data(stations-fixed-width.txt).
 *
 * @author Younggue Bae
 */
public class NcdcStationNameJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(NcdcStationNameJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    enum Temperature {
        MISSING,
        MALFORMED
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NcdcStationNameJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("input", "i", "The path for job input(ncdc)", true);
        addOption("output", "o", "The path for job output", true);
        addOption("minTemperature", null, "The minimun temperature to truncate", true);

        parseArguments(args);

        // input path
        Path inputPath = new Path(getOption("input"));

        // output path
        Path outputPath = new Path(getOption("output"));

        Configuration conf = getConf();
        conf.getInt("minTemperature", Integer.parseInt(getOption("minTemperature")));

        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME_PREFIX + getClass().getSimpleName());
        job.setJarByClass(NcdcStationNameJob.class);
        job.setMapOutputKeyClass(TextPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(NcdcStationNameMapper.class);
        job.setReducerClass(NcdcStationNameReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper.
     */
    public static class NcdcStationNameMapper extends Mapper<LongWritable, Text, TextPairWritable, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        private int minTemperature = Integer.MIN_VALUE;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            minTemperature = conf.getInt("minTemperature", Integer.MIN_VALUE);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);

            // Apply threshold
            if (parser.getAirTemperature() >= minTemperature) {
                context.write(new TextPairWritable(parser.getStationId(), parser.getYear()),
                        new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * Reducer.
     */
    public static class NcdcStationNameReducer extends Reducer<TextPairWritable, IntWritable, Text, IntWritable> {
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

            String stationId = key.getFirst().toString();
            String year = key.getSecond().toString();
            String stationName = stationNames.get(stationId);

            for (IntWritable val : values) {
                context.write(new Text(stationId + "\t" + stationName + "\t" + year), val);
            }
        }
    }

}
