package com.ls.mapreduce.telflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 计算出每个电话的流量,并且按照地区分组
 */
public class TelFlowMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File outPutFile = new File(args[1]);
        if(outPutFile.exists()){
            outPutFile.delete();
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"telFloe");
        //设置job 主程序
        job.setJarByClass(TelFlowMain.class);
        //设置输入 map class
        job.setMapperClass(FlowMapper.class);
        //设置输出 reduce class
        job.setReducerClass(FlowReducer.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(4);

        //设置输入key value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输出 key value class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定原始文件在的目录
        FileInputFormat.addInputPath(job,new Path(args[0]));


        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        if(b){
            System.out.println("true");
        }else {
            System.out.println("false");
        }

    }

}
