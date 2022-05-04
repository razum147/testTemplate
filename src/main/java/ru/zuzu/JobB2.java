package ru.zuzu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.StringTokenizer;

public class JobB2 {

    public static class TokenizerMapper
            extends Mapper<Text, Text, Text, IntWritable> {

        private Set<String> patternsToMerge = new HashSet<>();

        @SuppressWarnings("deprecation")
        protected void setup(Context context) throws java.io.IOException,
                InterruptedException {

            try {
                URI[] localPaths = context.getCacheFiles();
                for (URI test : localPaths) {
                    System.out.println(test);
                }
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
                    patternsToMerge.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            for (String category : patternsToMerge) {
                context.write(new Text(category + ":" + key), new IntWritable(Integer.parseInt(value.toString())));
            }
        }
    }
}
