package com.demo.hadoopd.mapReduce.partationner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class ProvincePartitioner extends Partitioner<Text, FlowSumBean> {
    @Override
    public int getPartition(Text text, FlowSumBean flowSumBean, int i) {
        String preNum = text.toString().substring(0, 3);
        int  partition = 4;

        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }
}
