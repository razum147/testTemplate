package ru.zuzu;

import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        super();
        this.first = new Text();
        this.second = new Text();
    }

    public TextPair(String first, String second) {
        super();
        this.first = new Text(first);
        this.second = new Text(second);
//        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public int compareTo(TextPair o) {
        int cmp = this.second.compareTo(o.second);
        if (cmp != 0)
            return cmp;
        return this.first.compareTo(o.first);
    }

//    public static class TextPairComparator extends WritableComparator {
//
//        protected TextPairComparator() {
//            super(TextPair.class, true);
//        }
//
//        @Override
//        public int compare(WritableComparable w1, WritableComparable w2) {
//            TextPair ip1 = (TextPair) w1;
//            TextPair ip2 = (TextPair) w2;
//            int cmp = ip1.compareTo(ip2);
////            if (cmp != 0) {
//            return cmp;
////            }
////            return ip2.getRating().compareTo(ip1.getRating()); //reverse
//        }
//    }
}
