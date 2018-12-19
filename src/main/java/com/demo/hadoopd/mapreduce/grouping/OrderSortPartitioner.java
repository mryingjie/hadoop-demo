package com.demo.hadoopd.mapreduce.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class OrderSortPartitioner extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        return (orderBean.getOrderId() & 2147483647) % numReduceTasks;
    }
}
