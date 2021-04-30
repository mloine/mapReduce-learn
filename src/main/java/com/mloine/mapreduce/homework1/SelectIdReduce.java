package com.mloine.mapreduce.homework1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @Author mloine
 * @Description
 * @Date 4:28 下午 2021/4/30
 */
public class SelectIdReduce extends Reducer<Text,Text,Text,Text> {

    private final String TARGET = "bee";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          if(TARGET.equals(key.toString())){
              for (Text x:values
                   ) {
                    context.write(x,null);
              }
          }
    }

}
