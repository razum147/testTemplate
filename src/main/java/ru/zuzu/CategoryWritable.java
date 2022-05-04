package ru.zuzu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CategoryWritable implements Writable {
    //    private Text category;
//    private IntWritable totalAge;
    private List<Text> reviewList;

    //default constructor for (de)serialization
    public CategoryWritable() {
//        category = new Text("");
        reviewList = new ArrayList<Text>();
//        totalAge = new IntWritable(0);
    }

    public void write(DataOutput dataOutput) throws IOException {
//        category.write(dataOutput); //write familyId
//        totalAge.write(dataOutput); //write totalAge
        dataOutput.writeInt(reviewList.size());  //write size of list
        for (int index = 0; index < reviewList.size(); index++) {
            reviewList.get(index).write(dataOutput); //write all the value of list
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
//        category.readFields(dataInput); //read familyId
//        totalAge.readFields(dataInput); //read totalAge
        int size = dataInput.readInt(); //read size of list
        reviewList = new ArrayList<Text>(size);
        for (int index = 0; index < size; index++) { //read all the values of list
            Text text = new Text();
            text.readFields(dataInput);
            reviewList.add(text);
        }
    }

//    public IntWritable getTotalAge() {
//        return totalAge;
//    }

//    public void setTotalAge(IntWritable totalAge) {
//        this.totalAge = totalAge;
//    }

//    public Text getCategory() {
//        return category;
//    }

//    public void setCategory(Text category) {
//        this.category = category;
//    }

    public List<Text> getReviewList() {
        return reviewList;
    }

    public void setReviewList(List<Text> reviewList) {
        this.reviewList = reviewList;
    }

//    public CategoryWritable(Text category, List<Text> reviewList) {
//        this.category = category;
//        this.reviewList = reviewList;
//    }

    public void addReviewerText(Text reviewText) {
        this.reviewList.add(reviewText);
    }

//    public void addTotalAge(IntWritable totalAge) {
//        this.totalAge.set(this.totalAge.get() + totalAge.get());
//    }

    @Override
    public String toString() {
        return reviewList.toString()
                .replace("[", "")
                .replace("]", "");
    }
}
