package com.breeze.mr_demo.simpleMoving;

import com.breeze.mr_demo.simpleMoving.simple.SimpleMovingAverage;
import com.breeze.tool.DateUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class SortByMRFMovingAverageReducer extends MapReduceBase implements Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    int windowSize = 5; // default window size

    /**
     * will be run only once get parameters from Hadoop's configuration
     */
    @Override
    public void configure(JobConf jobconf) {
        this.windowSize = jobconf.getInt("moving.average.window.size", 5);
    }

    public void reduce(CompositeKey key,
                       Iterator<TimeSeriesData> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter)
            throws IOException {

        // note that values are sorted.
        // apply moving average algorithm to sorted timeseries
        Text outputKey = new Text();
        Text outputValue = new Text();
        SimpleMovingAverage ma = new SimpleMovingAverage(this.windowSize);
        while (values.hasNext()) {
            TimeSeriesData data = values.next();
            ma.addNewNumber(data.getValue());
            double movingAverage = ma.getMovingAverage();
            long timestamp = data.getTimestamp();
            String dateAsString = DateUtil.getDateAsString(timestamp);
            //THE_LOGGER.info("Next number = " + x + ", SMA = " + sma.getMovingAverage());
            outputValue.set(dateAsString + "," + movingAverage);
            outputKey.set(key.getName());
            output.collect(outputKey, outputValue);
        }
        //
    }

}
