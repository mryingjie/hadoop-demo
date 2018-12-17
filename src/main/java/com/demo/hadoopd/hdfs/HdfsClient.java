package com.demo.hadoopd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/13
 * @Description
 */
public class HdfsClient{
    /**
     * 创建文件夹
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
//         configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
//         FileSystem fs = FileSystem.get(configuration);
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop.abc6.net:9000"), configuration, "root");

        // 创建目录
        //fs.mkdirs(new Path("/1208/zheng/ban"));

        //上传文件
        //fs.copyFromLocalFile(new Path("C:\\Users\\10910\\Desktop\\hello.txt"), new Path("/usr/local/input/hello.txt"));

        //下载文件
        //fs.copyToLocalFile(true, new Path("/usr/local/input/hello.txt"), new Path("C:\\Users\\10910\\Desktop\\hello1.txt"), true);

        //删除文件夹
        //fs.deleteOnExit(new Path("/1208"));

        //更改文件名
        //fs.rename(new Path("/usr/local/input/text.txt"), new Path("/usr/local/input/text1.txt"));

        //查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++");
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            System.out.println("是否是文件："+status.isFile());
            // 输出详情
            // 文件名称
            System.out.println("文件路径："+status.getPath());
            System.out.println("文件名："+status.getPath().getName());
            // 长度
            System.out.println("长度: "+status.getLen());
            // 权限
            System.out.println("权限:"+status.getPermission());
            // 组
            System.out.println("组:"+status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println("主机节点："+host);
                }
            }
            System.out.println(status);
            System.out.println("----------------分割线-----------");
        }

        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }


    @Test
    public void testIO() throws Exception {

        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop.abc6.net:9000"), entries, "root");

        //文件上传
        //获取输入流
//        FileInputStream in = new FileInputStream(new File("C:\\Users\\10910\\Desktop\\hello.txt"));
//        //获取输出流
//        FSDataOutputStream out = fs.create(new Path("/usr/local/hello.txt"));
//
//        //流的拷贝
//        IOUtils.copyBytes(in, out, entries);


        //文件下载
//        //获取输入流
        FSDataInputStream in = fs.open(new Path("/usr/local/hello.txt"));

        //获取输出流
        FileOutputStream out = new FileOutputStream("C:\\Users\\10910\\Desktop\\hello1.txt");

        IOUtils.copyBytes(in, out, entries);


//        文件的定位读取

        //关闭资源
        IOUtils.closeStream(in);
//        IOUtils.closeStream(out);
        fs.close();
        System.out.println("over");

    }

    public static void main(String[] args) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(new File("C:\\Users\\10910\\Desktop\\Desktop.zip"));

        FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\10910\\Desktop\\Desktop1.zip"));

        int len;
        byte[] b = new byte[1024];
        while ((len = fileInputStream.read(b))>=0){
            fileOutputStream.write(b,0,len);
            fileOutputStream.flush();
        }

//        org.apache.commons.io.IOUtils.copy(fileInputStream, fileOutputStream);

//
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fileOutputStream);

        fileInputStream.close();
        fileOutputStream.close();
    }

}

