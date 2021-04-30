package com.mloine.mapreduce.homoworkone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author mloine
 * @Description 
 * @Date 4:28 下午 2021/4/30
 */
public class SelectIdOneReduce extends Reducer<IntWritable, NullWritable,IntWritable,NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
