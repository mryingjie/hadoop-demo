package com.demo.hadoopd.mapReduce.mapjoin;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/19
 * @Description
 */
public class DistributedCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Map cache = new HashMap();

    private Text k = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileInputStream fileInputStream = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        BOMInputStream bis = null;
        try{
            fileInputStream = new FileInputStream("pd.txt");
            bis = new BOMInputStream(fileInputStream);
            isr = new InputStreamReader(bis,"UTF-8");
            br = new BufferedReader(isr);
            String line;
            while(StringUtils.isNotEmpty(line = br.readLine())){
                String[] split = line.split("\t");
                cache.put(split[0],split[1] );
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(br!=null){
                IOUtils.closeStream(br);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String s = split[1];
        String s1 = (String) cache.get(s);
        String strK = split[0]+"\t"+s1+"\t\t"+split[2];
        k.set(strK);
        context.write(k, NullWritable.get());
        k.clear();
    }
}


