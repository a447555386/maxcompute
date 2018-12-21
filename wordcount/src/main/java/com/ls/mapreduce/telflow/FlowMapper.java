package com.ls.mapreduce.telflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        String telNo = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long dFlow =Long.parseLong(fields[2]);
        FlowBean flowBean = new FlowBean(upFlow, dFlow);
        context.write(new Text(telNo),flowBean);
    }
}
