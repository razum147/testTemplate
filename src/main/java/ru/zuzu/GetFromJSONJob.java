package ru.zuzu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class GetFromJSONJob {

    public static class TokenizerMapperWithJson
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            try {
                JSONObject obj = new JSONObject(value.toString());

                // let's extract a field: URL, for this example
//                context.write(new Text("reviewerID123"), new Text(obj.get("reviewerID").toString()));// хрень, ид записи (мб есть дубли, хз)
//                context.write(new Text("asin123"), new Text(obj.get("asin").toString()));// уник ид продукта
//                context.write(new Text("helpful123"), new Text(obj.get("helpful").toString()));// важно [3,40] - оценка коммента
                context.write(new Text("result"), new Text(obj.get("reviewText").toString().toLowerCase()));//хрень
//                context.write(new Text("overall123"), new Text(obj.get("overall").toString()));//важно, еще важнее, если у автора helpful пиздатый
//                context.write(new Text("summary123"), new Text(obj.get("summary").toString()));//хрень, что в сумме словами - сюда только если нейронку какую крутить
//                context.write(new Text("unixReviewTime123"), new Text(obj.get("unixReviewTime").toString()));//тоже хрень, но мб важно по актуальности товара - когда комментил
//                context.write(new Text("reviewTime123"), new Text(obj.get("reviewTime").toString()));//аналогично верхнему
//                context.write(new Text("category123"), new Text(obj.get("category").toString()));//категория продукта - asin его частный случай
//                context.write(new Text("reviewerName123"), new Text(obj.get("reviewerName").toString()));//хрень - кто комментил, может быть нулем

            } catch (JSONException e) {
                System.out.println(e);
            }
        }
    }

//    public static class IntSumReducer
//            extends Reducer<Text, Text, Text, Text> {
//
//        Text valueToEmit = new Text();
//
//        public void reduce(Text key, Iterable<Text> values, Context context)
//                throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
//            for (Text x : values) {
//                sb.append(x.toString()).append("|");
//            }
//
//            valueToEmit.set(sb.substring(0, sb.length() - 1)); //to remove the last ','
//            context.write(key, valueToEmit);
//        }
//    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setNumReduceTasks(1); //столько партиций и редюсеров создает
//        job.setJarByClass(Tests.class);
//        job.setMapperClass(TokenizerMapperWithJson.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
