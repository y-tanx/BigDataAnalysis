package com.org.Classification.NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * NavieBayesTrain类：训练朴素贝叶斯分类算法模型
 */
public class NaiveBayesTrain {
    private static final int one = 1;
    private static final int classNum = 2;  // 类别的个数
    private static final int attributeNum = 20; // 属性的个数
    private static String classType = "CLASS";  // 类别键标签
    private static String attributeType = "ATTRIBUTE"; // 属性键标签

    public static class NavieBayesTrainMapper extends Mapper<LongWritable, Text, NaiveBayesKey, IntWritable> {

        /**
         * 从记录中提取类别和属性的分类信息
         *
         * @param key 文本行的字节偏移量
         * @param value 当前文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 构造记录的字段数组
            String line = value.toString();
            String[] fields = line.split(",");
            double[] record = new double[attributeNum + 1]; // 每个记录有20个属性 + 标签
            for(int i = 0; i < attributeNum + 1; i++) {
                record[i] = Double.parseDouble(fields[i]);
            }

            int classID = (int) record[attributeNum]; // 类别在第21个列
            // 提取该记录的类别信息
            NaiveBayesKey classKey = new NaiveBayesKey(classType, classID);
            context.write(classKey, new IntWritable(one));

            // 提取该记录的各个属性信息
            for(int i = 0; i < attributeNum; i++) {
                boolean isPositive = record[i] > 0;
                NaiveBayesKey attributeKey = new NaiveBayesKey(attributeType, classID, i, isPositive);
                context.write(attributeKey, new IntWritable(one));
            }
        }
    }

    public static class NavieBayesTrainReducer extends Reducer<NaiveBayesKey, IntWritable, Text, IntWritable> {
        private int[] classStats;
        private int[][][] attributeStats;
        private MultipleOutputs mos;

        /**
         * 初始化类别统计数组，属性统计数组，上下文输出对象
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void setup(Context context) throws IOException, InterruptedException{
            // 初始化类别统计数组
            classStats = new int[classNum];

            // 初始化属性统计数组
            attributeStats = new int[classNum][attributeNum][2];

            // 初始化MultipleOutputs
            mos = new MultipleOutputs(context);
        }

        /**
         * 统计各个类别的记录数，统计各个类别中各个属性的正负情况
         *
         * @param key 类别信息键或者属性信息键
         * @param values 键的数量
         * @param context MapReduce的上下文对象
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void reduce(NaiveBayesKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            String type = key.getType();;
            if(classType.equals(type)){ // 类别的信息键
                int classID = key.getClassID();

                // 统计类别的个数
                for(IntWritable value : values){
                    classStats[classID]++;
                }
            }else if(attributeType.equals(type)){
                int classID = key.getClassID();
                int attributeID = key.getAttributeID();
                int condition = (key.isPositive() == true) ? 1 : 0; // 属性值为正，对应下标1；属性值为负，对应下标0

                // 统计类别classID中，属性attributeID值的正负情况
                for(IntWritable value : values){
                    attributeStats[classID][attributeID][condition]++;
                }
            }
        }

        /**
         * 输出类别的记录数 与 类别的各个属性的正负情况
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将各个类别包含的记录数信息输出到"class_stats"文件
            for(int i = 0; i < classNum; i++){
                String classID = Integer.toString(i);
                mos.write("class", new Text(classID), new IntWritable(classStats[i]), "class_stats");
            }

            // 输出各个类别的各个属性的正负情况到 "attribute_stats" 文件
            for (int i = 0; i < classNum; i++) {
                for (int j = 0; j < attributeNum; j++) {
                    for (int k = 0; k < 2; k++) {
                        // 每次重新构建字符串，避免拼接冗余
                        StringBuilder sb = new StringBuilder();
                        // 拼接类别、属性和正负情况
                        sb.append(i).append("\t")    // 类别
                                .append(j).append("\t")    // 属性
                                .append(k);               // 正负情况

                        // 输出每个类别-属性-正负情况的统计结果
                        mos.write("attribute", new Text(sb.toString()) , new IntWritable(attributeStats[i][j][k]), "attribute_stats");
                    }
                }
            }

            // 关闭mos
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/train";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/train_Done";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000");

        // 删除上一次的作业
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir)){
            fs.delete(outputDir);
        }
        fs.close();

        // 创建MapReduce作业
        Job job = Job.getInstance(conf, "NavieBayes");
        // 指定Mapper类和Reducer类
        job.setMapperClass(NaiveBayesTrain.NavieBayesTrainMapper.class);
        job.setReducerClass(NaiveBayesTrain.NavieBayesTrainReducer.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(NaiveBayesKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 注册两个输出路径
        MultipleOutputs.addNamedOutput(job, "class", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "attribute", TextOutputFormat.class, Text.class, IntWritable.class);

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}