package com.breeze.mr_demo.combination;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    private static final Text reduceKey = new Text();

    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    int numberOfPairs; // 由setup()读取，由驱动器设置

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        List<String> items = convertItemsToList(line);
        if (items == null || items.size() == 0) {
            return;
        }

        generateMapperOutput(this.numberOfPairs, items, context);
    }

    private static List<String> convertItemsToList(String line) {
        if ((line == null) || (line.length() == 0)) {
            return null;
        }

        String[] tokens = StringUtils.split(line, ",");

        if ((tokens == null) || tokens.length == 0) {
            return null;
        }
        List<String> result = new ArrayList<String>(tokens.length);
        for (String token : tokens) {
            result.add(token);
        }
        return result;

    }

    private void generateMapperOutput(int numberOfPairs, List<String> items, Context context)
            throws IOException, InterruptedException {
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
        for (List<String> iterlists : sortedCombinations) {
            reduceKey.set(iterlists.toString());
            context.write(reduceKey, NUMBER_ONE);
        }

    }
}
