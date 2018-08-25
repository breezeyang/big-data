package com.breeze.mr_demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
		long counter = 0;
		for (LongWritable i : v2s) {
			counter += i.get();
		}
		context.write(key, new LongWritable(counter));
	}

}