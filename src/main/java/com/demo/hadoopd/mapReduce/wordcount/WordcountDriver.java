package com.demo.hadoopd.mapReduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/14
 * @Description
 */
public class WordcountDriver{

    public static void main(String[] args) {

        //1 获取job信息
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            //2 获取jar信息
            job.setJarByClass(WordcountDriver.class);

            //3 关联mappper和reducer
            job.setMapperClass(WordcountMapper.class);
            job.setReducerClass(WordcountReducer.class);

            //4 设置map输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //5 设置最终输出数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //6 设置数据输入和输出文件路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //7 提交代码
            boolean b = job.waitForCompletion(true);
            System.exit(b?0:1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
