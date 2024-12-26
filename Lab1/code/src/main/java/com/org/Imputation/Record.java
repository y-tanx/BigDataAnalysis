package com.org.Imputation;

/**
 * 记录每种依赖属性组合的Income和Rating的数量与总和，用于计算每种依赖属性组合的Income和Rating的平均值
 */
public class Record {
    int num;    // 某一种依赖属性组合含有的记录数量
    double sum; // 某一种依赖属性组合中，所有记录的Income/Rating的总和

    /**
     * Record的构造函数
     */
    public Record(int num, double sum) {
        this.sum = sum;
        this.num = num;
    }
    public int getNum()
    {
        return this.num;
    }

    public double getSum()
    {
        return this.sum;
    }

    public void setNum(int num)
    {
        this.num = num;
    }

    public void setSum(double sum)
    {
        this.sum = sum;
    }
}
