package com.breeze.mr_demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 接收数据V1
		String line = value.toString();
		String[] words = line.split(" ");
		for (String w : words) {
			// 出现一次记一个1
			context.write(new Text(w), new LongWritable(1));
		}
	}
}