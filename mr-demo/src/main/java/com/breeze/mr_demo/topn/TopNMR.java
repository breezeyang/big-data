package com.breeze.mr_demo.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class TopNMR {

    public static int N = 10;

    public static class TopNMapper extends Mapper<Object, Text, NullWritable, IntWritable> {

        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void map(Object key, Text value, Context context) {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }

        protected void cleanup(Context context) {
            for (Integer i : repToRecordMap.keySet()) {
                try {
                    context.write(NullWritable.get(), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            for (Integer i : repToRecordMap.keySet()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: TopN" + "<input-path> <output-path>");
        }
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];

        removeOutput(conf, output);
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopNMR.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class); // map阶段的输出的key
        job.setMapOutputValueClass(IntWritable.class); // map阶段的输出的value

        job.setOutputKeyClass(Text.class); // reduce阶段的输出的key
        job.setOutputValueClass(IntWritable.class); // reduce阶段的输出的value

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void removeOutput(Configuration conf, String output) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(output), conf);
        Path path = new Path(output);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();
    }

}
