package com.ls.mapreduce.telflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();
    static {
        provinceDict.put("130", 0);
        provinceDict.put("181", 1);
        provinceDict.put("137", 2);
    }

    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prex = text.toString().substring(0,3);
        Integer provinceId = provinceDict.get(prex);
        return provinceId == null?4:provinceId;
    }
}
