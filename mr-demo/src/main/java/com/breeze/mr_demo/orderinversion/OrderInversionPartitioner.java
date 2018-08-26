package com.breeze.mr_demo.orderinversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {

    @Override
    public int getPartition(PairOfWords pair, IntWritable value, int number) {
        // 使具有相同左词的所有WordPai对象被发送到同一个reducer
        return Math.abs(pair.getLeftElement().hashCode() % number);
    }
}
