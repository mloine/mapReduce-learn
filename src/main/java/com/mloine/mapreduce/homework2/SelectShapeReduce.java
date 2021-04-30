package com.mloine.mapreduce.homework2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelectShapeReduce extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum  = 0;
        for (IntWritable x:values
             ) {
            sum += x.get();
        }

        result.set(sum);
        context.write(key,result);

    }
}
