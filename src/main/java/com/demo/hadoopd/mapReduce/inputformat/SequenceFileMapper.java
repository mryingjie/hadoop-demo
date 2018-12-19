package com.demo.hadoopd.mapReduce.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/18
 * @Description
 */
public class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable> {

    private Text key = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit =
                (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        key.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        System.out.println("==================执行map方法===============");
        context.write(this.key, value);
    }
}
