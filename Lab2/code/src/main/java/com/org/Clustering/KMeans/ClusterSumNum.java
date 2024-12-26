package com.org.Clustering.KMeans;

/**
 * SumNumWritable类：统计每个类别中包含的记录总数 和 所有记录的各列的和
 */
public class ClusterSumNum {
    private int num;
    private double[] sum;

    /**
     * SumNumWritable有参构造函数，用于初始化SumNumWritable
     *
     * @param num 类别中包含的记录的个数
     * @param sum 列和数组，类别中所有记录各个列的和
     */
    public ClusterSumNum(int num, double[] sum) {
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
    public void merge(ClusterSumNum other){
        this.num += other.num;
        int row = this.sum.length;
        for(int i = 0; i < row; i++){
            this.sum[i] += other.sum[i];
        }
    }
}
