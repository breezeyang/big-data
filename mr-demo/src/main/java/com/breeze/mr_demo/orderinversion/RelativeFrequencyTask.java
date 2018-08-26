package com.breeze.mr_demo.orderinversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelativeFrequencyTask {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: RelativeFrequencyTask" + "<input-path> <output-path>");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "RelativeFrequency");
        
        job.setJarByClass(RelativeFrequencyTask.class);
        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(OrderInversionPartitioner.class);

        // 设置Reduce任务数
        job.setNumReduceTasks(10);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            System.out.println("MR run successfully");

        } else {
            System.out.println("MR run failed");

        }
    }

}
