package com.org.Clustering.KMeans;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * SumNumWritable类：统计每个类别中包含的记录总数 和 所有记录的各列的和
 */
public class SumNumWritable implements Writable {
    private int num;
    private double[] sum;

    /**
     * SumNumWritable无参构造函数，供hadoop反射调用
     */
    public SumNumWritable(){};

    /**
     * SumNumWritable有参构造函数，用于初始化SumNumWritable
     *
     * @param num 类别中包含的记录的个数
     * @param sum 列和数组，类别中所有记录各个列的和
     */
    public SumNumWritable(int num, double[] sum) {
        this.num = num;
        this.sum = sum.clone(); // 深拷贝
    }

    /**
     * 获得类别中记录的个数num
     *
     * @return int 记录的个数num
     */
    public int getNum(){
        return this.num;
    }

    /**
     * 获得记录的列和sum
     *
     * @return double[] 记录的列和sum
     */
    public double[] getSum() {
        return this.sum.clone();
    }

    public void setNum(int num) {
        this.num = num;
    }

    public void setSum(double[] sum) {
        this.sum = sum.clone();
    }

    /**
     * 合并SumNumWritable
     *
     * @param other 要合并的SumNumWritable
     */
    public void merge(SumNumWritable other){
        this.num += other.num;
        int row = this.sum.length;
        for(int i = 0; i < row; i++){
            this.sum[i] += other.sum[i];
        }
    }

    /**
     * 数据的序列化，将Writable对象转换为字节流
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);  // 先写入记录的个数
        dataOutput.writeInt(sum.length);    // 再写入sum数组的大小
        for(double d : sum) {
            dataOutput.writeDouble(d);  // 写入sum数组中的每个值
        }
    }

    /**
     * 数据的反序列化，将字节流转换为Writable对象
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.num = dataInput.readInt();   // 先读取记录的个数
        int size = dataInput.readInt();    // 再读取sum数组的大小
        for(int i = 0; i < size; i++) {
            sum[i] = dataInput.readDouble();    // 读入sum数组的每个值
        }
    }
}
