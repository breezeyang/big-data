package com.breeze.mr_demo.simpleMoving;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SortByMRFDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        JobConf jobconf = new JobConf(conf, SortByMRFDriver.class);
        jobconf.setJobName("SortByMRFMovingAverageDriver");

        String[] otherArgs = new String[3];
        otherArgs[0] = "2";
        otherArgs[1] = args[1];
        otherArgs[2] = args[2];
        if (otherArgs.length != 3) {
            System.err.println("Usage: SortByMRFMovingAverageDriver <window_size> <input> <output>");
            System.exit(1);
        }

        // add jars to distributed cache
        //  HadoopUtil.addJarsToDistributedCache(conf, "/lib/");

        // set mapper/reducer
        jobconf.setMapperClass(SortByMRFMovingAverageMapper.class);
        jobconf.setReducerClass(SortByMRFMovingAverageReducer.class);

        // define mapper's output key-value
        jobconf.setMapOutputKeyClass(CompositeKey.class);
        jobconf.setMapOutputValueClass(TimeSeriesData.class);

        // define reducer's output key-value
        jobconf.setOutputKeyClass(Text.class);
        jobconf.setOutputValueClass(Text.class);

        // set window size for moving average calculation
        int windowSize = Integer.parseInt(otherArgs[0]);
        jobconf.setInt("moving.average.window.size", windowSize);

        // define I/O
        FileInputFormat.setInputPaths(jobconf, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobconf, new Path(otherArgs[2]));

        jobconf.setInputFormat(TextInputFormat.class);
        jobconf.setOutputFormat(TextOutputFormat.class);
        jobconf.setCompressMapOutput(true);

        // the following 3 setting are needed for "secondary sorting"
        // Partitioner decides which mapper output goes to which reducer
        // based on mapper output key. In general, different key is in
        // different group (Iterator at the reducer side). But sometimes,
        // we want different key in the same group. This is the time for
        // Output Value Grouping Comparator, which is used to group mapper
        // output (similar to group by condition in SQL).  The Output Key
        // Comparator is used during sort stage for the mapper output key.
        jobconf.setPartitionerClass(NaturalKeyPartitioner.class);
        jobconf.setOutputKeyComparatorClass(CompositeKey.CompositeKeyComparator.class);
        jobconf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);


        JobClient.runJob(jobconf);
    }


}