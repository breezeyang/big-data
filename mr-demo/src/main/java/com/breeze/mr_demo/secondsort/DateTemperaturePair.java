package com.breeze.mr_demo.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair>{

    private String yearMonth;  // 自然键
    private String day;
    private Integer temperature; // 次键
    
    public int compareTo(DateTemperaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(o.getTemperature());
        }
        return -1;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, yearMonth);
        out.writeInt(temperature);
        
    }

    public void readFields(DataInput in) throws IOException {
        this.yearMonth = Text.readString(in);
        this.temperature = in.readInt();
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return yearMonth.toString();
    }
    
}

