package com.ls.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置对象
        Configuration conf = new Configuration();

        //创建job对象
        Job job = Job.getInstance(conf,"wordCount");

        //设置job运行的类
        job.setJarByClass(App.class);

        //设置mapper
        job.setMapperClass(WordcountMapper.class);

        //设置reducer
        job.setReducerClass(WordcountReducer.class);

        //设置map 输出的key value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer out输出的key 和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径 file:///d:mr" file:///d:mr/out
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交job
        job.waitForCompletion(true);

        System.out.println("Hello World!");
    }
}
