package com.nextmining.course.hadoop.linereview;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nextmining.hadoop.io.TextPairWritable;
import com.nextmining.hadoop.mapreduce.AbstractJob;
import com.nextmining.nlp.NLPTools;
import com.nextmining.nlp.Sentence;
import com.nextmining.nlp.Token;
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
import java.util.*;

/**
 * Word counting job by the users' rate of the LINE ios app store reviews.
 *
 * @author Younggue Bae
 */
public class LineReviewWordCountByRateJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(LineReviewWordCountByRateJob.class);

    private static final String JOB_NAME_PREFIX = "[ygbae]";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LineReviewWordCountByRateJob(), args);
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
        job.setJarByClass(LineReviewWordCountByRateJob.class);
        job.setMapOutputKeyClass(TextPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(LineReviewWordCountMapper.class);
        job.setReducerClass(LineReviewWordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, outputPath);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper.
     */
    public static class LineReviewWordCountMapper
            extends Mapper<LongWritable, Text, TextPairWritable, IntWritable> {

        // 텍스트에서 주요 단어 키워드들을 추출하기 위해 사용할 NLP 툴
        private NLPTools nlpTools = new NLPTools();

        // JSON 파싱을 위한 툴
        private ObjectMapper jsonMapper = new ObjectMapper();

        // 불용어(stopword) 사전
        private Set<String> stopwords = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Configuration conf = context.getConfiguration();

            this.stopwords = loadStopwords("stopwords_en.txt");
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String jsonValue = value.toString();

            int rating = this.getRate(jsonValue);
            // 평점 정보가 없는 경우 skip 한다.
            if (rating == 0) {
                return;
            }
            String reviewText = this.getReviewText(jsonValue);

            List<String> keywords = this.getKeywords(reviewText);

            for (String keyword : keywords) {
                context.write(new TextPairWritable(String.valueOf(rating), keyword), new IntWritable(1));
            }
        }

        /**
         * 불용어(stopword) 사전을 가져온다.
         *
         * @param file
         * @return
         * @throws IOException
         */
        private Set<String> loadStopwords(String file) throws IOException {
            Set<String> stopwords = new HashSet<String>();

            BufferedReader in =
                    new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));

            System.err.println("Stopwords:");
            String line;
            while ((line = in.readLine()) != null) {
                // right trim
                line = line.replaceAll("\\s+$", "");
                if (!line.equals("")) {
                    stopwords.add(line.toLowerCase());
                }
            }

            in.close();

            return stopwords;
        }

        /**
         * 평점 정보를 가져온다.
         *
         * @param jsonText
         * @return
         */
        private int getRate(String jsonText) {
            try {
                TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
                };
                Map<String, Object> data = jsonMapper.readValue(jsonText, typeRef);

                if (data.get("rating") != null) {
                    return (Integer) data.get("rating");
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }

            return 0;
        }

        /**
         * 사용자 리뷰 텍스트를 가져온다.
         *
         * @param jsonText
         * @return
         */
        private String getReviewText(String jsonText) {
            try {
                TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
                };
                Map<String, Object> data = jsonMapper.readValue(jsonText, typeRef);

                if (data.get("text") != null) {
                    return (String) data.get("text");
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }

            return "";
        }

        /**
         * 사용자 리뷰 텍스트에서 자연어처리 과정을 거쳐 주요 키워드들만 추출한다.
         *
         * @param text
         * @return
         */
        private List<String> getKeywords(String text) {
            List<String> keywords = new ArrayList<String>();
            try {
                Sentence sentence = nlpTools.analyzeSentence(text);
                List<Token> tokens = sentence.getKeywordTokens();

                for (Token token : tokens) {
                    String word = token.getToken();
                    // 불용어 사전에 포함된 단어는 제외한다.
                    if (word != null && !stopwords.contains(word)) {
                        // 소문자로 변환한 뒤 키워드리스트에 추가한다.
                        keywords.add(word.toLowerCase());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return keywords;
        }
    }

    /**
     * Reducer.
     */
    public static class LineReviewWordCountReducer
            extends Reducer<TextPairWritable, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(TextPairWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String rate = key.getFirst().toString();
            String word = key.getSecond().toString();

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(new Text(rate + "\t" + word),
                    new IntWritable(sum));
        }
    }

}
