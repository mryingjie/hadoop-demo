package com.demo.hadoopd.mapReduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/14
 * @Description
 */
public class FlowSumReducer extends Reducer<Text,FlowSumBean,Text,FlowSumBean> {

    private Text k = new Text();



    @Override
    protected void reduce(Text key, Iterable<FlowSumBean> values, Context context) throws IOException, InterruptedException {

        int upFlowSum = 0;

        int downFlowSum = 0;

        for (FlowSumBean value : values) {
            upFlowSum += value.getUpFlow();
            downFlowSum += value.getDownFlow();
        }
        k.set(key);
        FlowSumBean flowSumBean = new FlowSumBean(upFlowSum,downFlowSum);
        context.write(k, flowSumBean);
        k.clear();
    }
}
