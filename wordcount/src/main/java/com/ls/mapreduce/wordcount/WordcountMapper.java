package com.ls.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//输入key类型、输入value类型、输出key类型、输出value类型。
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //得到每一行数据
        String line = value.toString();
        //分割
        String[] split = line.split(" ");
        for (String word : split) {
            context.write(new Text(word),new IntWritable(1));
        }


    }
}
