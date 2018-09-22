package com.breeze.mr_demo.simpleMoving;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.breeze.tool.DateUtil;

public class SortInMemoryMovingAverageMapper extends Mapper<LongWritable, Text, Text, TimeSeriesData> {

    private final Text reducerKey = new Text();

    private final TimeSeriesData reducerValue = new TimeSeriesData();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        if ((record == null) || (record.length() == 0)) {
            return;
        }
        String[] tokens = StringUtils.split(record.trim(), ",");
        if (tokens.length == 3) {
            Date date = DateUtil.getDate(tokens[1]);// 2004-11-04,
            if (date == null) {
                return;
            }
            reducerKey.set(tokens[0]); // GOOG
            reducerValue.set(date.getTime(), Double.parseDouble(tokens[2]));
            context.write(reducerKey, reducerValue);
        } else {
            // log as error, not enough tokens
        }
    }

}
