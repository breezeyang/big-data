package com.breeze.mr_demo.secondsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortingReducer extends Reducer<DateTemperaturePair, IntWritable, DateTemperaturePair, Text> {

    @Override   
    protected void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder sortedTemperatureList = new StringBuilder();
        for (IntWritable temperature : values) {
            sortedTemperatureList.append(temperature);
            sortedTemperatureList.append(",");
        }
        sortedTemperatureList.deleteCharAt(sortedTemperatureList.length() - 1);
        context.write(key, new Text(sortedTemperatureList.toString()));
    }

}
