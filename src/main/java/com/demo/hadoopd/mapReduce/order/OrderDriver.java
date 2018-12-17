package com.demo.hadoopd.mapReduce.order;

import com.demo.hadoopd.mapReduce.flowsum.FlowSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class OrderDriver {

    public static void main(String[] args) {
        //1 获取job信息
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            //2 获取jar信息
            job.setJarByClass(OrderDriver.class);

            //3 关联mappper和reducer
            job.setMapperClass(OrderMapper.class);
            job.setReducerClass(OrderReducer.class);

            //4 设置map输出数据类型
            job.setMapOutputKeyClass(OrderBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            //设置combainer 一般与reducer一样
//            job.setCombinerClass(OrderReducer.class);

            //设置分组
            job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

            //5 设置最终输出数据类型
            job.setOutputKeyClass(OrderBean.class);
            job.setOutputValueClass(NullWritable.class);

            //6 设置数据输入和输出文件路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //设置分片机制 将输入的大量小文件合并成一个切片统一处理
//            job.setInputFormatClass(TextInputFormat.class); //默认
//            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setInputFormatClass(CombineTextInputFormat.class);
            // 4m
            CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
            // 2m
            CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

            //设置分区 默认分区
            job.setPartitionerClass(OrderSortPartitioner.class);
            //设置最终输出的文件数 应大于等于分区数
            job.setNumReduceTasks(3);

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
