package ru.zuzu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class CategoryTextsJob {
    public static class CategoryTextsJobMapper
            extends Mapper<Object, Text, Text, CategoryWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            try {
                JSONObject obj = new JSONObject(value.toString());

                // let's extract a field: URL, for this example
//                context.write(new Text("reviewerID123"), new Text(obj.get("reviewerID").toString()));// хрень, ид записи (мб есть дубли, хз)
//                context.write(new Text("asin123"), new Text(obj.get("asin").toString()));// уник ид продукта
//                context.write(new Text("helpful123"), new Text(obj.get("helpful").toString()));// важно [3,40] - оценка коммента
//                context.write(new Text("categoryText"), new Text(obj.get("reviewText").toString().toLowerCase()));//хрень
//                context.write(new Text("overall123"), new Text(obj.get("overall").toString()));//важно, еще важнее, если у автора helpful пиздатый
//                context.write(new Text("summary123"), new Text(obj.get("summary").toString()));//хрень, что в сумме словами - сюда только если нейронку какую крутить
//                context.write(new Text("unixReviewTime123"), new Text(obj.get("unixReviewTime").toString()));//тоже хрень, но мб важно по актуальности товара - когда комментил
//                context.write(new Text("reviewTime123"), new Text(obj.get("reviewTime").toString()));//аналогично верхнему
//                context.write(new Text("categories"), new Text(obj.get("category").toString().toLowerCase()));//категория продукта - asin его частный случай
//                context.write(new Text("reviewerName123"), new Text(obj.get("reviewerName").toString()));//хрень - кто комментил, может быть нулем
                CategoryWritable categoryWritable = new CategoryWritable();
                categoryWritable.addReviewerText(new Text(obj.get("reviewText").toString().toLowerCase()));
                context.write(new Text(obj.get("category").toString().toLowerCase()), categoryWritable);
            } catch (JSONException e) {
                System.out.println(e);
            }
        }
    }

    public static class CategoryTextsJobReducer
            extends Reducer<Text, CategoryWritable, Text, CategoryWritable> {

        CategoryWritable categoryWritable = new CategoryWritable();

        public void reduce(Text key, Iterable<CategoryWritable> values, Context context)
                throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
//            categoryWritable.setCategory(key);
            for (CategoryWritable x : values) {
                categoryWritable.addReviewerText(x.getReviewList().get(0));
//                sb.append(x.toString()).append("|");
            }

//            valueToEmit.set(sb.substring(0, sb.length() - 1)); //to remove the last ','
            context.write(key, categoryWritable);
        }
    }
}
