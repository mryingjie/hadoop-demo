package com.demo.hadoopd.mapReduce.wholefile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/18
 * @Description
 */
public class WholeFileInputformat  extends FileInputFormat<NullWritable,BytesWritable>{
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader
            (InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileRecordReader wholeFileRecordReader = new WholeFileRecordReader();
        wholeFileRecordReader.initialize(inputSplit, context);
        return wholeFileRecordReader;
    }

    /**
     * 是否分片 要将小文件合并为一个文件 不分片
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
