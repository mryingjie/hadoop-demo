package com.demo.hadoopd.mapReduce.wholefile;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/18
 * @Description
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,Text> {

    private Text value = new Text();

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("==================执行reduce方法===============");
        value.set(new String(values.iterator().next().getBytes()));
        context.write(key, value);
        value.clear();
    }
}
