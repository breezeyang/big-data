package com.breeze.mr_demo.orderinversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;
    private final PairOfWords pair = new PairOfWords();
    IntWritable ONE = new IntWritable(1);
    IntWritable totalCount = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");

        if ((tokens == null) || (tokens.length < 2)) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            String word = tokens[i];
            pair.setLeftElement(word);
            int start = 0;
            if (i - neighborWindow >= 0) {
                start = i - neighborWindow;
            }
            int end = 0;
            if (i + neighborWindow >= tokens.length) {
                end = tokens.length - 1;
            } else {
                end = i + neighborWindow;
            }

            for (int j = start; j <= end; j++) {
                if (i == j) {
                    continue;
                }
                pair.setRightElement(tokens[j]);
                context.write(pair, ONE);

            }
            pair.setRightElement("*");
            totalCount.set(end - start);
            context.write(pair, totalCount);
        }
    }

}