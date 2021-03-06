package com.breeze.mr_demo.secondsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortingMapper extends Mapper<LongWritable, Text, DateTemperaturePair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String[] tokens = value.toString().split(",");
        if (tokens.length  < 3) {
            return ;
        }
        // YYYY = tokens[0]
        // MM = tokens[1]
        // DD = tokens[2]
        // temperature = tokens[3]
        String yearMonth = tokens[0] + "-" + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);

        DateTemperaturePair reduceKey = new DateTemperaturePair();
        reduceKey.setYearMonth(yearMonth);
        reduceKey.setDay(day);
        reduceKey.setTemperature(temperature);
        context.write(reduceKey, new IntWritable(temperature));
    }
}