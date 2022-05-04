package ru.zuzu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChiTestJob {

    public static class ChiJobAMapper
            extends Mapper<Text, IntWritable, Text, Text> {

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {

            context.write(key, new Text("A: " + value.toString()));
        }
    }

    public static class ChiJobBMapper
            extends Mapper<Text, IntWritable, Text, Text> {


        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {

            context.write(key, new Text("B: " + value.toString()));
        }
    }

    public static class ChiJobCMapper
            extends Mapper<Text, IntWritable, Text, Text> {


        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {

            context.write(key, new Text("C: " + value.toString()));
        }
    }

    public static class ChiJobReducer
            extends Reducer<Text, Text, Text, Text> {

        private Pattern pattern = Pattern.compile("[ABC]");

        private Double S = 0D;

        @SuppressWarnings("deprecation")
        protected void setup(Context context) throws java.io.IOException,
                InterruptedException {

            try {
                URI[] localPaths = context.getCacheFiles();
                System.out.println(localPaths);
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
                    S = Double.parseDouble(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
//            StringBuilder result = new StringBuilder();
//            for (Text test : values) {
//                result.append(test);
//            }
            Double A = 0D;
            Double B = 0D;
            Double C = 0D;
            Double chiSquare = 0D;

            for (Text str : values) {
                Matcher matcher = pattern.matcher(str.toString());
                while (matcher.find()) {
                    switch (str.toString().substring(matcher.start(), matcher.end())) {
                        case "A":
                            A = Double.parseDouble(str.toString().replaceAll("\\D", ""));
                            break;
                        case "B":
                            B = Double.parseDouble(str.toString().replaceAll("\\D", ""));
                            break;
                        case "C":
                            C = Double.parseDouble(str.toString().replaceAll("\\D", ""));
                            break;
                    }
                }

                Double D = S - (A + (B - A) + C); //B - не все слова, а в количество комментов
                System.out.println("S: " + S + " A: " + A + " B: " + B + " C: " + C + " D: " + D);

                Double numerator = S * Math.pow(((A * D) - ((B - A) * C)), 2);
                Double denominator = (A + (B - A)) * (A + C) * ((B - A) + D) * (C + D);
                chiSquare = numerator / denominator;
//                chiValue.set(chiSquare);

            }
            context.write(key, new Text(chiSquare.toString()));

//            context.write(key, new Text(result.toString()));
        }
    }


}
