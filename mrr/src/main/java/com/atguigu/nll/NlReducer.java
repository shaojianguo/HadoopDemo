package com.atguigu.nll;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NlReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
