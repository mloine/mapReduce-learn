package com.mloine.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author mloine
 * @Description
 *  处理map端shuffle过来的数据,对数据进行二次聚合
 *  KEYIN,VALUEIN,KEYOUT,VALUEOUT
 *  KEYIN   map端输出的key的类型
 *  VALUEIN map端输出的value的类型
 *  KEYOUT reduce输出到hdfs的key的类型
 *  VALUEOUT reduce输出到hdfs的valueOut数据类型
 *
 *  (The,2)
 *  (Apache,5)
 *
 *  所有map任务执行完成后才会执行reduce 任务
 *  @Date 10:56 上午 2021/4/30
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * （The,1）
     *  (hello,1)
     *  (Apache,1)
     *  (The,1)
     *  (Tomcat,1)
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value:values) {
            sum += value.get();
        }
        result.set(sum);
        //reduce 阶段将结果集保存在hdfs上
        context.write(key,result);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
