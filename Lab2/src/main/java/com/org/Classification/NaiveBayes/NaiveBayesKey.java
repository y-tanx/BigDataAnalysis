package com.org.Classification.NaiveBayes;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * NavieBayesKey类：在朴素贝叶斯分类的训练过程，存放类别或属性的信息
 */
public class NaiveBayesKey implements WritableComparable<NaiveBayesKey> {
    private String type;
    private int classID;
    private int attributeID;
    private boolean isPositive;

    // 默认构造函数，供Hadoop反序列化需要
    public NaiveBayesKey() {}

    // 类别键的构造函数
    public NaiveBayesKey(String type, int classID) {
        this.type = type;
        this.classID = classID;
        this.attributeID = -1;  // 默认值，对于CLASS类型，attributeID无效
        this.isPositive = false; // 默认值
    }

    // 属性键的构造函数
    public NaiveBayesKey(String type, int classID, int attributeID, boolean isPositive) {
        this.type = type;
        this.classID = classID;
        this.attributeID = attributeID;
        this.isPositive = isPositive;
    }

    public String getType() {
        return this.type;
    }

    public int getClassID() {
        return this.classID;
    }

    public int getAttributeID() {
        return this.attributeID;
    }

    public boolean isPositive() {
        return this.isPositive;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.type);
        dataOutput.writeInt(this.classID);
        dataOutput.writeInt(this.attributeID);
        dataOutput.writeBoolean(this.isPositive);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.classID = dataInput.readInt();
        this.attributeID = dataInput.readInt();
        this.isPositive = dataInput.readBoolean();
    }

    /**
     * 实现 compareTo 方法，用于排序
     */
    @Override
    public int compareTo(NaiveBayesKey other) {
        // 先比较类型
        int result = this.type.compareTo(other.type);
        if (result != 0) {
            return result;
        }
        // 然后比较类别ID
        result = Integer.compare(this.classID, other.classID);
        if (result != 0) {
            return result;
        }
        // 然后比较属性ID
        result = Integer.compare(this.attributeID, other.attributeID);
        if (result != 0) {
            return result;
        }
        // 最后比较属性正负值
        return Boolean.compare(this.isPositive, other.isPositive);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NaiveBayesKey that = (NaiveBayesKey) o;
        return classID == that.classID &&
                attributeID == that.attributeID &&
                isPositive == that.isPositive &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, classID, attributeID, isPositive);
    }

    @Override
    public String toString() {
        return "NaiveBayesKey{" +
                "type='" + type + '\'' +
                ", classID=" + classID +
                ", attributeID=" + attributeID +
                ", isPositive=" + isPositive +
                '}';
    }
}
