package com.demo.hadoopd.mapReduce.partationner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/14
 * @Description
 */
public class FlowSumDriver {

    public static void main(String[] args) {

        //1 获取job信息
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            //2 获取jar信息
            job.setJarByClass(FlowSumDriver.class);

            //3 关联mappper和reducer
            job.setMapperClass(FlowSumMapper.class);
            job.setReducerClass(FlowSumReducer.class);

            //4 设置map输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowSumBean.class);

            //设置combainer 一般与reducer一样
            job.setCombinerClass(FlowSumReducer.class);

            //5 设置最终输出数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowSumBean.class);

            //6 设置数据输入和输出文件路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //设置切片机制 默认
            job.setInputFormatClass(TextInputFormat.class);

            //设置分区
            job.setPartitionerClass(ProvincePartitioner.class);
//            指定相应数量的reduce task
            job.setNumReduceTasks(5);

            // 提交代码
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
