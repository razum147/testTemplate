package ru.zuzu;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class GetCategoriesJob {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            try {
                JSONObject obj = new JSONObject(value.toString());

                // let's extract a field: URL, for this example
//                context.write(new Text("reviewerID123"), new Text(obj.get("reviewerID").toString()));// хрень, ид записи (мб есть дубли, хз)
//                context.write(new Text("asin123"), new Text(obj.get("asin").toString()));// уник ид продукта
//                context.write(new Text("helpful123"), new Text(obj.get("helpful").toString()));// важно [3,40] - оценка коммента
//                context.write(NullWritable.get(), new Text(obj.get("reviewText").toString().toLowerCase()));//хрень
//                context.write(new Text("overall123"), new Text(obj.get("overall").toString()));//важно, еще важнее, если у автора helpful пиздатый
//                context.write(new Text("summary123"), new Text(obj.get("summary").toString()));//хрень, что в сумме словами - сюда только если нейронку какую крутить
//                context.write(new Text("unixReviewTime123"), new Text(obj.get("unixReviewTime").toString()));//тоже хрень, но мб важно по актуальности товара - когда комментил
//                context.write(new Text("reviewTime123"), new Text(obj.get("reviewTime").toString()));//аналогично верхнему
                context.write(new Text(obj.get("category").toString().toLowerCase()), NullWritable.get());//категория продукта - asin его частный случай
//                context.write(new Text("reviewerName123"), new Text(obj.get("reviewerName").toString()));//хрень - кто комментил, может быть нулем

            } catch (JSONException e) {
                System.out.println(e);
            }
        }
    }


    public static class WordReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }
}
