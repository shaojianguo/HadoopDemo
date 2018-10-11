package com.atguigu.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class DistributedCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    HashMap<String,String> pdMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path)), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
        reader.close();
    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        String pId = fields[1];
        String pdName = pdMap.get(pId);

        k.set(fields[0]+"\t"+pdName+"\t"+fields[1]);

        context.write(k,NullWritable.get());
    }
}
