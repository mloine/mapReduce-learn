package com.mloine.mapreduce.homework1;

import com.mloine.mapreduce.wordcount.WordCountMapper;
import com.mloine.mapreduce.wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @Author mloine
 * @Description 使用MR 实现  select id from table where name = "bee"
 *  10, bee, sphere, black, 2010-03-03T04:15:26
 * 20,bee,disc,white,2010-04-03T04:15:26
 * 30,cat,ball,red,2010-04-03T04:15:26
 * 40,dog,cube,green,2010-05-03T04:15:26
 * 50,dog,sphere,blue,2010-06-03T04:15:26
 * 60,fish,cone,black,2010-07-03T04:15:26
 * 70,cat,ball,red,2010-03-03T04:15:26
 * @Date 4:29 下午 2021/4/30
 */
public class SelectIdDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new SelectIdDriver(),args));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //1.创建Configuration对象
        Configuration conf = getConf();

        conf.set("fs.defaultFS","hdfs://hadoop001:9000");

        Job job = Job.getInstance(conf, "SelectIdMR");
        job.setJarByClass(this.getClass());

        //input path 添加输入
        FileInputFormat.addInputPath(job,new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        //out path  添加输出
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        //map class
        job.setMapperClass(SelectIdMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Combiner class 在map阶段先进行数据的本地聚合 （可选 用于本地聚合减少传输数据提高MR执行效率）
//        job.setCombinerClass(SelectIdReduce.class);

        //reduce class
        job.setReducerClass(SelectIdReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
