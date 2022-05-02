//package ru.zuzu;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//import java.util.StringTokenizer;
//
//public class JobChi {
//
//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, IntWritable> {
//
//        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            lasdfhaskdfhaskjd
//        }
//    }
//
//    public static class ReducerChi
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        public void reduce(TextPair key, Iterable<TextPairLong> value, Context context) throws IOException, InterruptedException {
//            long A = 0; //количество отзывов в текущей категории, содержащих это слово
//            long B = 0; //количество отзывов вне этой категории, но содержащий это слово
//            long C = 0; //количество отзывов в этой категории, но без этого слова
//            long D; //количество отзывов вне этой категории без этого слова
//            while (value.iterator().hasNext()) {
//                TextPairLong p = value.iterator().next();
//                switch (p.getU().toString()) {
//                    case "A":
//                        A = p.getV().get();
//                        break;
//                    case "B":
//                        B = p.getV().get();
//                        break;
//                    case "C":
//                        C = p.getV().get();
//                        break;
//                }
//            }
//            D = S - (A + B + C);
//
//            double numerator = S * Math.pow(((A * D) - (B * C)), 2);
//            double denominator = (A + B) * (A + C) * (B + D) * (C + D);
//            double chiSquare = numerator / denominator;
//            chiValue.set(chiSquare);
//
//            context.write(key, chiValue);
//        }
//
//    }
//}
//
//}
