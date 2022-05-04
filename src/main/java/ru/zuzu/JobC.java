package ru.zuzu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class JobC {

    public static class JobCMapper
            extends Mapper<Text, CategoryWritable, Text, IntWritable> {

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
                        if (!text.toString().contains(word)) {
                            count++;
                        }
                    }
                    context.write(new Text(key.toString() + ":" + word), new IntWritable(count.intValue())); //какого-то черта тут появляется слово aba, которого нет в wordlist
                }

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

//    public static class JobCReducer
//            extends Reducer<NullWritable, Text, NullWritable, Text> {
//
//        Text valueToEmit = new Text();
//        BigInteger val = new BigInteger("0");
//
//        public void reduce(NullWritable key, Iterable<Text> values, Context context)
//                throws IOException, InterruptedException {
//            for (Text x : values) {
//                val = val.add(BigInteger.ONE);
//
//            }
//
//            valueToEmit.set(val.toString());
////            valueToEmit.set(sb.substring(0, sb.length() - 1)); //to remove the last ','
//            context.write(NullWritable.get(), valueToEmit);
//        }
//    }
}

