package com.demo.hadoopd.mapReduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/14
 * @Description
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowSumBean> {

    private Text k = new Text();

    private FlowSumBean v = new FlowSumBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        1363157993055 	13560436666	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99		18	15	1116		954		200
//        手机号码										                                    上行流量   下行流量

        String str = value.toString();
        String[] split = str.split("\t");

        k.set(split[0]);
        long upFlow = Long.parseLong(split[split.length-3]);
        long downFlow = Long.parseLong(split[split.length-2]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow(upFlow+downFlow);
        context.write(k, v);

    }

}
