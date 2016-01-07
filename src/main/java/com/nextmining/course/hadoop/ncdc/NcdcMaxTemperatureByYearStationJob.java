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
 */
public class NcdcMaxTemperatureByYearStationJob extends AbstractJob {

	@Override
	public int run(String[] args) throws Exception {
		
		
		return 0;
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
            String year = parser.getYear();
            String stationId = parser.getStationId();
            int airTemperature = parser.getAirTemperature();
            
            if (parser.isValidTemperature()) {
                // --------------> START
                /*
                 * --------------------------------------------------------------
                 * 연도(year)-기상대ID(station id)별로 기온을 write하세.
                 * -------------------------------------------------------------
                 */
            	
            	
            	// <-------------- END
               
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

            String stationId = key.getSecond().toString();
            String stationName = stationNames.get(stationId);

            // --------------> START
            /*
             * --------------------------------------------------------------
             * 최고 기온을 계산해 보세요.
             * -------------------------------------------------------------
             */
             int maxValue = Integer.MIN_VALUE;
        	
        	
             
        	// <-------------- END
            
            context.write(new Text(year + "\t" + stationId + "\t" + stationName),
                    new IntWritable(maxValue));
        }
    }
}
