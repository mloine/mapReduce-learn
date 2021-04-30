package com.mloine.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author mloine
 * @Description
 *  1.继承Mapper类
 *      KEYIN, VALUEIN, KEYOUT, VALUEOUT
 *      KEYIN 读取改行数据的偏移量
 *      VALUEIN 当前行的数据类型
 *      KEYOUT 数据传输到reduce阶段的key类型
 *      VALUEOUT 数据传输到reduce阶段的value类型
 *  2.重写map函数
 *  3.map方法会被执行哼多次 每次执行会读取一行数句将数据集封装成k-v形式
 *   The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers
 *   simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and
 *     key  value
 *   （102） The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers
 *   （204） simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and
 *
 *   4.写到reduce阶段的数据类型 K V
 *   (The,1)
 *   (Hadoop,1)
 * @Date 10:56 上午 2021/4/30
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    /**
     * 处理数据的业务逻辑 开发人员常用map会运行很多次
     * 将数据变换为如下格式
     *  (The,1)
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(" ");
        for(String x : words){
            word.set(x);
            // (The,1) (Hadoop,1)
            context.write(word,ONE);
        }
    }

    /**
     * 在map任务运行的时候，调用一次，且只调用一次。可以用来创建connection等
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        URI[] cacheFiles = context.getCacheFiles();
//        Path path = new Path(cacheFiles[0]);
//        File customerType =  new File(String.valueOf(path),"./dept.txt");

        super.setup(context);
    }

    /**
     * 在map任务结束的时候，调用一次，可以关闭数据库的连接等操作
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


}
