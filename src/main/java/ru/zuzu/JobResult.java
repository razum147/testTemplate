package ru.zuzu;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class JobResult {
    public static class JobResultMapper
            extends Mapper<Text, Text, Text, TextPair> {

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {

            try {
                String[] splittedText = value.toString().split(":");
                context.write(new Text(key.toString().toLowerCase()), new TextPair(splittedText[0], splittedText[1]));

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public static class JobResultReducer
            extends Reducer<Text, TextPair, Text, Text> {

        public void reduce(Text key, Iterable<TextPair> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<TextPair> array = new ArrayList<>();
//            Collections.sort(values);
            for ( TextPair test : values) {
                array.add(test);
            }
            Collections.sort(array, Collections.reverseOrder());
            StringBuilder resultValues = new StringBuilder();
            for (int i =0; i < 150; i++) {
                resultValues.append(array.get(i).getFirst().toString() + ":" + array.get(i).getSecond().toString() + " ");
            }
            context.write(key, new Text(resultValues.toString()));
//            context.write(key, new Text("test"));
        }
    }

//    public static class EmployeeComparator extends WritableComparator {
//        protected EmployeeComparator() {
//            super(Employee.class, true);
//        }
//        @Override
//        public int compare(WritableComparable one, WritableComparable two) {
//            Employee emp1 = (Employee) one;
//            Employee emp2 = (Employee) two;
//            int compare = emp2.getSalary().intValue() - emp1.getSalary().intValue();
//            if (compare == 0) {
//                compare = emp1.getFname().compareTo(emp2.getFname());
//            }
//            return compare;
//        }
//    }
}
