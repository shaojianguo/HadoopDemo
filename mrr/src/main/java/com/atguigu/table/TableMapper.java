package com.atguigu.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    String name;
    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (name.startsWith("order")){
            String[] split = line.split("\t");
            v.setOrder_id(split[0]);
            v.setP_id(split[1]);
            v.setAmount(Integer.parseInt(split[2]));
            v.setPname("");
            v.setFlag("order");

            k.set(split[1]);
        }else {
            String[] split = line.split("\t");
            v.setOrder_id("");
            v.setP_id(split[0]);
            v.setAmount(0);
            v.setPname(split[1]);
            v.setFlag("pd");

            k.set(split[0]);
        }
        context.write(k,v);
    }
}
