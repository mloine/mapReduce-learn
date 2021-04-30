package com.mloine.mapreduce.homework1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @Author mloine
 * @Description 
 * @Date 4:28 下午 2021/4/30
 */
public class SelectIdMapper extends Mapper<LongWritable, Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(",");
        if(split.length < 2){return;}
        String id = split[0];
        String name = split[1];

        outKey.set(name);
        outValue.set(id);

        context.write(outKey,outValue);
    }
}
