package com.breeze.app_demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class URLCat {
	
	public static void main(String[] args) throws URISyntaxException, IOException {
	    String path = "hdfs://127.0.0.1:8900//user/tuan/demo/";
        URI uri = new URI(path);
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path + "mapdemo"));
        byte[] bytes = new byte[1024];
        int length = -1;
        while ((length = fsDataInputStream.read(bytes)) != -1) {
            String s = new String(bytes, 0, length);
            System.out.println(s);
        }

	}

}
