package com.breeze.mr_demo.simpleMoving;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class NaturalKeyPartitioner implements
        Partitioner<CompositeKey, TimeSeriesData> {

    @Override
    public int getPartition(CompositeKey key,
                            TimeSeriesData value,
                            int numberOfPartitions) {
        return Math.abs((int) (hash(key.getName()) % numberOfPartitions));
    }

    @Override
    public void configure(JobConf jobconf) {
    }

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
