package com.org.Classification.KNN;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClusterDistanceWritable类：保存测试样本到各个类别的距离
 */
public class ClassDistanceWritable implements Writable{
    private int classID;
    private double distance;

    public ClassDistanceWritable() {
    }

    /**
     * ClusterDistanceWritable的构造方法
     *
     * @param classID 类别
     * @param distance 测试样本到类别clusterID的距离
     */
    ClassDistanceWritable(int classID, double distance) {
        this.classID = classID;
        this.distance = distance;
    }

    public int getClassID() {
        return this.classID;
    }

    public double getDistance() {
        return this.distance;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.classID);   // 序列化类别ID
        out.writeDouble(this.distance); // 序列化距离
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.classID = in.readInt();  // 反序列化类别ID
        this.distance = in.readDouble();    // 反序列化距离
    }
}