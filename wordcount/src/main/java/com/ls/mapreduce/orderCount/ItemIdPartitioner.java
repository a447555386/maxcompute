package com.ls.mapreduce.orderCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<ItemBean, NullWritable> {


    public int getPartition(ItemBean itemBean, NullWritable nullWritable, int i) {
        return (itemBean.getItemId().hashCode() & Integer.MAX_VALUE)%i;
    }
}
