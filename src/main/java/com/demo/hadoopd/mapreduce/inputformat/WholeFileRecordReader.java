package com.demo.hadoopd.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/18
 * @Description
 */
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;

    private Configuration configuration;

    private Boolean processed = false;

    private BytesWritable values = new BytesWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            //1 定义缓冲区
            byte[] buff = new byte[(int) fileSplit.getLength()];

            FileSystem fileSystem = null;
            FSDataInputStream fis = null;
            try{
//                //2 获取文件系统
                Path path = fileSplit.getPath();
                fileSystem = path.getFileSystem(this.configuration);
//
//                //3 读取数据
                fis = fileSystem.open(path);
//                String substring = path.toString().substring(6);
//                fis = new FileInputStream(new File(substring));

                //4 读取文件内容
                IOUtils.readFully(fis, buff, 0, buff.length);

                // 5 输出文件内容
                values.set(buff, 0, buff.length);
                processed = true;
            }catch(Exception e){
                e.printStackTrace();
                return false;
            }finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fileSystem);
            }
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.values;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.processed? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) {
        File file  = new File("E:\\demo\\hadoopdemo\\src\\main\\resources\\input\\inputformat\\three.txt");
//        file.getPath();
        System.out.println(file.getPath());
    }
}
