package com.nextmining.course.hadoop.wordcount;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * MapReduce job driver for word count.
 *
 * @author Younggue Bae
 */
public class WordCountJob extends AbstractJob {

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    private WordCountJob() {
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("input", "i", "The path for job input(comma separated)", true);
        addOption("output", "o", "The path for job output", true);

        parseArguments(args);

        // Input path
        String[] inputs = getOption("input").split(",");
        Set<Path> inputPaths = new HashSet<Path>();
        for (String input : inputs) {
            inputPaths.add(new Path(input));
        }

        // Output path
        Path outputPath = new Path(getOption("output"));

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        // Job setting
        job.setJobName(JOB_NAME_PREFIX + getClass().getSimpleName());   // 맵리듀스 잡 이름
        job.setJarByClass(WordCountJob.class);                          // 잡 드라이버 클래스명
        job.setMapOutputKeyClass(Text.class);                           // 매퍼 출력 key 데이터타입
        job.setMapOutputValueClass(IntWritable.class);                  // 매퍼 출력 value 데이터타입
        job.setOutputKeyClass(Text.class);                              // 리듀서 출력 key 데이터타입
        job.setOutputValueClass(IntWritable.class);                     // 리듀서 출력 value 데이터타입
        job.setMapperClass(WordCountMapper.class);                      // 매퍼 클래스명
        job.setReducerClass(WordCountReducer.class);                    // 리듀서 클래스명
        job.setInputFormatClass(TextInputFormat.class);                 // 입력데이터 포맷
        job.setOutputFormatClass(TextOutputFormat.class);               // 출력데이터 포맷

        // Set input path
        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        // Set output path
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper.
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        /**
         * 보통의 경우 setup 함수는 구현할 필요가 없으며,
         * Job Driver로 부터 전달되는 파라미터 등을 Configuration 객체에서 꺼내서 사용할 필요가 있을 때 구현해 준다.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Configuration conf = context.getConfiguration();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer.
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * 보통의 경우 setup 함수는 구현할 필요가 없으며,
         * Job Driver로 부터 전달되는 파라미터 등을 Configuration 객체에서 꺼내서 사용할 필요가 있을 때 구현해 준다.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Configuration conf = context.getConfiguration();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}