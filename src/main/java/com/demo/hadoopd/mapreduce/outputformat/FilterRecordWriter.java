package com.demo.hadoopd.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/18
 * @Description
 */
public class FilterRecordWriter extends RecordWriter<Text,NullWritable> {

    private FSDataOutputStream atguiguOut = null;
    private FSDataOutputStream otherOut = null;

    public void initialize(TaskAttemptContext context) {
        // 1 获取文件系统
        FileSystem fs = null;

        try {
            fs = FileSystem.get(context.getConfiguration());

            // 2 创建输出文件路径
            String outputPath = context.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
            Path atguiguPath = new Path(outputPath+"\\atguigu.txt");
            Path otherPath = new Path(outputPath+"\\other.txt");

            // 3 创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fs);
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("atguigu")){
            atguiguOut.write(key.toString().getBytes());
        }else{
            otherOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (atguiguOut != null) {
            atguiguOut.close();
        }

        if (otherOut != null) {
            otherOut.close();
        }
    }

}
