package com.ls.mapreduce.telflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow =0;
        long dFlow = 0;
        for (FlowBean bean : values) {
            upFlow+=bean.getUpFlow();
            dFlow+=bean.getdFlow();
        }
        FlowBean flowBean = new FlowBean(upFlow, dFlow);
        context.write(key,flowBean);
    }
}
