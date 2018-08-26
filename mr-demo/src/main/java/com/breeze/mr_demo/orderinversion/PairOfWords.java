package com.breeze.mr_demo.orderinversion;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairOfWords implements WritableComparable<PairOfWords> {

    private String leftElement;
    private String rightElement;

    public PairOfWords() {

    }

    public PairOfWords(String left, String right) {
        leftElement = left;
        rightElement = right;
    }

    public String getLeftElement() {
        return leftElement;
    }

    public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public String getRightElement() {
        return rightElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }

    public void readFields(DataInput in) throws IOException {
        leftElement = in.readUTF();
        rightElement = in.readUTF();

    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(leftElement);
        out.writeUTF(rightElement);

    }

    public int compareTo(PairOfWords other) {
        // 排序，有*的排到前面，使其可以先算总数
        int returnVal = this.rightElement.compareTo(other.getRightElement());
        if (returnVal != 0) {
            return returnVal;
        }
        if (this.leftElement.toString().equals('*')) {
            return -1;
        } else if (other.getLeftElement().toString().equals('*')) {
            return 1;
        }
        return this.leftElement.compareTo(other.getLeftElement());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        //
        if (!(obj instanceof PairOfWords)) {
            return false;
        }
        //
        PairOfWords pair = (PairOfWords) obj;
        return leftElement.equals(pair.getLeftElement()) && rightElement.equals(pair.getRightElement());
    }

    @Override
    public int hashCode() {
        return leftElement.hashCode() + rightElement.hashCode();
    }

    @Override
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

}
