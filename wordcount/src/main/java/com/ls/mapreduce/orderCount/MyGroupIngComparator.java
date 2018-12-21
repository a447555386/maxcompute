package com.ls.mapreduce.orderCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupIngComparator extends WritableComparator {

    public MyGroupIngComparator() {
        super( ItemBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ItemBean item1 = (ItemBean)a;
        ItemBean item2 = (ItemBean)b;
        return item1.getItemId().compareTo(item2.getItemId());

    }
}
