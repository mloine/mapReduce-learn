package com.mloine.mapreduce.homoworkone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author mloine
 * @Description 
 * @Date 4:28 下午 2021/4/30
 */
public class SelectIdOneMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    private IntWritable outKey = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(",");

        if(split[1].equals("bee")){
            outKey.set(Integer.valueOf(split[0]));
            context.write(outKey,NullWritable.get());
        }



    }
}
