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

        // --------------> START
        /*
         * --------------------------------------------------------------
         * 아래에 맵리듀스 잡을 세팅하기 위한 코드를 작성하세요.
         *
         * job.setJobName();    // 맵리듀스 잡 이름
         * job.setJarByClass(); // 잡 드라이버 클래스명
         * job.setMapOutputKeyClass();  // 매퍼 출력 key 데이터타입
         * job.setMapOutputValueClass();    // 매퍼 출력 value 데이터타입
         * job.setOutputKeyClass(); // 리듀서 출력 key 데이터타입
         * job.setOutputValueClass();   // 리듀서 출력 value 데이터타입
         * job.setMapperClass();    // 매퍼 클래스명
         * job.setReducerClass();   // 리듀서 클래스명
         * job.setInputFormatClass();   // 입력데이터 포맷
         * job.setOutputFormatClass();  // 출력데이터 포맷
         * -------------------------------------------------------------
         */



        // <-------------- END

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
            // --------------> START
            /*
             * --------------------------------------------------------------
             * 맵 함수를 완성하세요.
             * 입력으로 들어온 value 텍스트에서 공백을 구분자로 tokenize하여 word를 추출한다.
             * 추출된 워드를 리듀서에서 카운트하기 위해 key, value를 출력한다.
             * -------------------------------------------------------------
             */




            // <-------------- END
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
            // --------------> START
            /*
             * --------------------------------------------------------------
             * 리듀스 함수를 완성하세요.
             * 입력으로 들어온 key(word), value(count)를 가지고 key별로 카운트를 합산한다.
             * key별 합산된 key, value를 최종 출력한다.
             * -------------------------------------------------------------
             */
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            // <-------------- END
        }
    }

}