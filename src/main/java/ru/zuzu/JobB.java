package ru.zuzu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;



public class JobB {

    public static class JobBMapper
            extends Mapper<Text, CategoryWritable, Text, Text> {

        private Set<String> searchWords = new HashSet<>();

        @SuppressWarnings("deprecation")
        protected void setup(Context context) throws java.io.IOException,
                InterruptedException {

            try {
                URI[] localPaths = context.getCacheFiles();
                parseFile(localPaths[0]);
            } catch (IOException e) {
                System.err.println("Exception reading stop word file: " + e);

            }

        }

        private void parseFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    searchWords.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Text key, CategoryWritable value, Context context
        ) throws IOException, InterruptedException {

            try {
                for (String word : searchWords) {
                    Long count = 0L;
                    for (Text text : value.getReviewList()) {
                        if (text.toString().contains(word)) {
                            count++;
                        }
                    }
                    context.write(new Text(word), new Text(count.toString())); //какого-то черта тут появляется слово aba, которого нет в wordlist
                }

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public static class JobBReducer
            extends Reducer<Text, Text, Text, Text> {
        Long val = 0L;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text x : values) {
                val+=Long.parseLong(x.toString());
            }
//
//            valueToEmit.set(val.toString());
////            valueToEmit.set(sb.substring(0, sb.length() - 1)); //to remove the last ','
            context.write(key, new Text(val.toString()));
        }
    }
}

