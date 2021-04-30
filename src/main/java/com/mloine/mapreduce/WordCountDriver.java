package com.mloine.mapreduce;

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
 * @Description
 *  驱动mapReduce程序
 * @Date 10:56 上午 2021/4/30
 */
public class WordCountDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new WordCountDriver(),args));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 驱动mapReduce的任务代码
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        //1.创建Configuration对象
        Configuration conf = getConf();

        conf.set("fs.defaultFS","hdfs://hadoop001:9000");

        Job job = Job.getInstance(conf, "WordCountMR");
        job.setJarByClass(this.getClass());

        //input path 添加输入
        FileInputFormat.addInputPath(job,new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        //out path  添加输出
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        //map class
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Combiner class 在map阶段先进行数据的本地聚合 （可选 用于本地聚合减少传输数据提高MR执行效率）
        job.setCombinerClass(WordCountReduce.class);

        //reduce class
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce数量
        //        job.setNumReduceTasks();
        // 增加缓存
//        job.addCacheFile(new URI("/user/data/dept.txt"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
