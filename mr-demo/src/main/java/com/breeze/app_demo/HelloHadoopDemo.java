package com.breeze.app_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

public class HelloHadoopDemo {

    public static String fileName;

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        createHDFS();
        readHDFS();
        return;
    }

    private static void createHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        String path = "hdfs://127.0.0.1:8900/first/hello/";
        URI uri = new URI(path);
        FileSystem fileSystem = FileSystem.get(uri, configuration, "tuan");// user不是root，没有权限写
        fileName = "file" + new Date().getTime();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path + fileName), false);
        String json = "hello hadoop";
        fsDataOutputStream.write(json.getBytes());
        fsDataOutputStream.close();
        System.out.println(fileName);
        // Thread.sleep(3000L);
    }

    private static void readHDFS() throws URISyntaxException, IOException, InterruptedException {
        String path = "hdfs://127.0.0.1:8900/first/hello/";
        URI uri = new URI(path);
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());// user不是root，没有权限写
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path + fileName));
        byte[] bytes = new byte[1024];
        int length = -1;
        while ((length = fsDataInputStream.read(bytes)) != -1) {
            String s = new String(bytes, 0, length);
            System.out.println(s);
        }
    }
}