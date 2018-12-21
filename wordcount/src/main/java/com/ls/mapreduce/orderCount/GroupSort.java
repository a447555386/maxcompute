package com.ls.mapreduce.orderCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupSort {

    static class SortMapper extends Mapper<LongWritable, Text,ItemBean, NullWritable>{
        ItemBean bean = new ItemBean();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            bean.set(new Text(fields[0]),new DoubleWritable(Double.parseDouble(fields[1])));
            context.write(bean,NullWritable.get());
        }
    }
    static class SortReducer extends Reducer<ItemBean,NullWritable,ItemBean,NullWritable>{


        @Override
        protected void reduce(ItemBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "groupSort");

        job.setJarByClass(GroupSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(ItemBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(ItemBean.class);

        job.setGroupingComparatorClass(MyGroupIngComparator.class);
        job.setPartitionerClass(ItemIdPartitioner.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        if (b){
            System.out.println("success");
        }else {
            System.out.println("false");
        }

    }


}
