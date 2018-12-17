package com.demo.hadoopd.mapReduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    private OrderBean key = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        this.key.setOrderId(Integer.parseInt(split[0]));
        this.key.setPrice(Double.parseDouble(split[2]));
        context.write(this.key, NullWritable.get());
    }
}
