package com.org.Clustering.KMeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordClusteridWritable implements Writable {
    private String record;
    private int clusterID;

    public RecordClusteridWritable() {
    }

    public RecordClusteridWritable(String record, int clusterID) {
        this.record = record;
        this.clusterID = clusterID;
    }

    public String getRecord() {
        return this.record;
    }

    public int getClusterID() {
        return this.clusterID;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.record);       // 序列化记录
        dataOutput.writeInt(this.clusterID);    // 序列化类别序号
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.record = dataInput.readUTF();
        this.clusterID = dataInput.readInt();
    }
}