package com.breeze.mr_demo.simpleMoving;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {

    private long timestamp;

    private double value;

    public TimeSeriesData() {
    }

    public TimeSeriesData(long timestamp, double value) {
        set(timestamp, value);
    }

    public static TimeSeriesData copy(TimeSeriesData tsd) {
        return new TimeSeriesData(tsd.timestamp, tsd.value);
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getValue() {
        return this.value;
    }

    public void set(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesData data) {
        if (this.timestamp < data.timestamp) {
            return -1;
        } else if (this.timestamp > data.timestamp) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.value);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.value = in.readDouble();

    }

    public static TimeSeriesData read(DataInput in) throws IOException {
        TimeSeriesData tsData = new TimeSeriesData();
        tsData.readFields(in);
        return tsData;
    }

    @Override
    public String toString() {
        return "(" + timestamp + "," + value + ")";
    }

}
