package com.demo.hadoopd.mapReduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/14
 * @Description
 *
 * 输入的key 行号
 * 输入的value Text
 * 输出的key 单词 Text
 * 输出的value 单词个数
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    private Text k = new Text();

    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将一行内容转换为String
        String s = value.toString();
        String[] split = s.split(" ");
        for (String s1 : split) {
            k.set(s1);
            context.write(k, v);
            k.clear();
        }
    }
}
