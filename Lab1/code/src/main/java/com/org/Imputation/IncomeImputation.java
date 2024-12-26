package com.org.Imputation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * 本类实现对Income的缺失值填充
 * @author myc
 * @version 1.0
 */
public class IncomeImputation {
    private static HashMap<IncomeDependices, Record> incomeMap = new HashMap<IncomeDependices, Record>();

    /**
     * Mapper类：统计Income的每种依赖属性组合中(user_nationality+user_career)，记录的数量与所有记录的Income之和
     */
    public static class IncomeImputationMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * 构造映射<Income的每种依赖属性组合，记录的数量与所有记录中Income之和>
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常*
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\|");
            // 获得user_income(11)字段
            String user_income = fields[11];
            // 如果user_income没有缺失，则进行统计
            if(user_income.charAt(0) != '?')
            {
                // 获得user_nationality(9)和user_career(10)字段
                String user_nationality = fields[9];
                String user_career = fields[10];
                // 创建income依赖属性组合
                IncomeDependices incomeDependices = new IncomeDependices(user_nationality, user_career);
                // 在哈希表中查找键值对
                Record record = incomeMap.get(incomeDependices);
                // 更新哈希表
                if (record != null) {
                    // 如果哈希表中存在这个依赖属性组合，则更新Record
                    record.setNum(record.getNum() + 1);
                    record.setSum(record.getSum() + Double.parseDouble(user_income));
                } else {
                    // 哈希表中不存在这个依赖属性组合，则创建Record，并加入到哈希表中
                    Record newRecord = new Record(1, Double.parseDouble(user_income));
                    incomeMap.put(incomeDependices, newRecord);
                }
            }
            context.write(new Text(line), new Text(""));
        }
    }

    /**
     * Reducer类：使用热卡填充的方法对income进行缺失值填充
     */
    public static class IncomeImputationReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 使用hamming距离，比较哈希表中每种依赖属性值组合 和 缺失income的记录中依赖属性组合的相似度，选择相似度最大的依赖属性组合
         * 用这一组依赖属性组合的平均值作为income的值
         *
         * @param key Mapper输入的career字段
         * @param values 对应职业的记录
         * @param context 上下文对象，用于输出抽样后的记录
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxSimilarity = -1;
            IncomeDependices bestDependencies = null;
            String line = key.toString();
            String[] fields = line.split("\\|");
            String user_income = fields[11];
            // reduce只对缺失的user_income处理
            if(user_income.charAt(0) == '?') {
                // 获得样本中income的依赖属性组合
                String sample_nationality = fields[9];
                String sample_career = fields[10];
                // 遍历哈希表中的每个键值对，用hamming距离比较相似度
                for(IncomeDependices dependice : incomeMap.keySet()) {
                    int similarity = 0; // 这一组依赖属性组合与样本的依赖属性组合的相似度
                    String map_nationality = dependice.getNationality();
                    String map_career = dependice.getCareer();
                    // 用hamming距离计算相似度
                    if(map_career.equals(sample_career))
                    {
                        similarity++;
                    }
                    if(map_nationality.equals(sample_nationality))
                    {
                        similarity++;
                    }
                    // 更新最大相似度和对应的依赖属性组合
                    if(similarity > maxSimilarity) {
                        maxSimilarity = similarity;
                        bestDependencies = dependice;
                    }
                }
                // 对缺失值填充
                if(bestDependencies != null)
                {
                    Record record = incomeMap.get(bestDependencies);
                    double averageIncome = record.getSum() / record.getNum();
                    line = line.replace(user_income, String.format("%.2f", averageIncome));
                }
            }
            context.write(new Text(line), new Text(""));
        }
    }

    /**
     * MapReduce入口方法，配置作业参数并启动任务
     *
     * @param args 命令行参数
     * @throws Exception MapReduce作业启动抛出的异常
     *
     */
    public static void main(String args[]) throws Exception {
        // 定义文件输入输出路径
        String inputPath = "hdfs://myc-ubuntu:9000/Lab1/D_FormatAndNormalize";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Done";
        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 设置默认文件系统为HDFS
        // 获取HDFS文件系统对象
        FileSystem fs = FileSystem.get(conf);
        // 删除原文件夹（如果存在）
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir))
        {
            fs.delete(outputDir, true);
        }
        // 创建一个MapReduce作业
        Job job = Job.getInstance(conf, "IncomeImputation");
        // 指定Mapper类和Reducer类
        job.setMapperClass(IncomeImputation.IncomeImputationMapper.class);
        job.setReducerClass(IncomeImputation.IncomeImputationReducer.class);
        // 指定maptask输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 制定reducetask输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }
}

/**
 * 记录Income的依赖属性键组合
 */
class IncomeDependices{
    String user_nationality;
    String user_career;

    /**
     * IncomeDependices的构造函数
     */
    public IncomeDependices(String user_nationality, String user_career) {
        this.user_nationality = user_nationality;
        this.user_career = user_career;
    }

    public String getNationality(){
        return this.user_nationality;
    }

    public String getCareer(){
        return this.user_career;
    }

    public void setNationality(String nationality){
        this.user_nationality = nationality;
    }

    public void setCareer(String career){
        this.user_career = career;
    }
}
