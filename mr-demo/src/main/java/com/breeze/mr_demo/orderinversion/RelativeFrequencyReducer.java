package com.breeze.mr_demo.orderinversion;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NOT_DEFINED";

    @Override
    public void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws java.io.IOException, InterruptedException {
        if (key.getRightElement().equals("*")) {
            if (key.getLeftElement().equals(currentWord)) {
                totalCount += totalCount + getTotalCount(values);
            } else {
                currentWord = key.getLeftElement();
                totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            relativeCount.set((double) count / totalCount);
            context.write(key, relativeCount);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}