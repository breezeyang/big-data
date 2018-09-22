package com.breeze.mr_demo.simpleMoving;

import com.breeze.tool.DateUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SortInMemoryMovingAverageReducer extends Reducer<Text, TimeSeriesData, Text, Text> {

    private int windowSize = 4; // 默认值

    @Override
    public void setup(Context context) {
        this.windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
        System.out.println("setup(): key=" + windowSize);
    }

    public void reduce(Text key, Iterable<TimeSeriesData> values, Context context)
            throws IOException, InterruptedException {

        System.out.println("reduce(): key=" + key.toString());

        // build the unsorted list of timeseries
        List<TimeSeriesData> timeseries = new ArrayList<TimeSeriesData>();
        for (TimeSeriesData tsData : values) {
            TimeSeriesData copy = TimeSeriesData.copy(tsData);
            timeseries.add(copy);
        }

        // sort the timeseries data in memory and
        // apply moving average algorithm to sorted timeseries
        Collections.sort(timeseries);
        System.out.println("reduce(): timeseries=" + timeseries.toString());


        // calculate prefix sum
        double sum = 0.0;
        for (int i = 0; i < windowSize - 1; i++) {
            sum += timeseries.get(i).getValue();
        }

        // now we have enough timeseries data to calculate moving average
        Text outputValue = new Text(); // reuse object
        for (int i = windowSize - 1; i < timeseries.size(); i++) {
            System.out.println("reduce(): key=" + key.toString() + "  i=" + i);
            sum += timeseries.get(i).getValue();
            double movingAverage = sum / windowSize;
            long timestamp = timeseries.get(i).getTimestamp();
            outputValue.set(DateUtil.getDateAsString(timestamp) + "," + movingAverage);
            // send output to HDFS
            context.write(key, outputValue);

            // prepare for next iteration
            sum -= timeseries.get(i - windowSize + 1).getValue();
        }
    } // reduce
}
