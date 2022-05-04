package ru.zuzu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class FilteringJob {

    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Text> {

        private Text word = new Text();

        private Set<String> patternsToSkip = new HashSet<>();

        @SuppressWarnings("deprecation")
        protected void setup(Context context) throws java.io.IOException,
                InterruptedException {

            try {
                URI[] localPaths = context.getCacheFiles();
                parseSkipFile(localPaths[0]);
            } catch (IOException e) {
                System.err.println("Exception reading stop word file: " + e);

            }

        }

        private void parseSkipFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n" +
                    "\n\f|()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/1234567890<>^");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.toString().isEmpty() || patternsToSkip.contains(word.toString())) {
                    continue;
                }
                context.write(NullWritable.get(), word);
            }
        }
    }

    public static class WordReducer
            extends Reducer<NullWritable, Text, NullWritable, Text> {

        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> test = new ArrayList();
            for (Text word : values) {
                test.add(word.toString());
            }
            Collections.sort(test);
            for (String word : test) {
                context.write(NullWritable.get(), new Text(word));
            }
        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setNumReduceTasks(2);
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
