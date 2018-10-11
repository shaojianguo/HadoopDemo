package com.atguigu.nll;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NlMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");

        for (String s : split) {
            k.set(s);
            context.write(k,v);
        }
    }
}
