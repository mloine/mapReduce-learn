package com.mloine.mapreduce.homework2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author mloine
 * @Description
 * @Date 5:09 下午 2021/4/30
 */
public class SelectShapeMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outKey = new Text();
    private final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        outKey.set(value.toString().split(",")[2]);
        context.write(outKey,ONE);
    }
}
