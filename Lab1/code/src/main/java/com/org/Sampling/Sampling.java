package com.org.Sampling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 该类实现分层抽样算法
 * 根据职业(career)字段进行分层，并对每个职业类别的数据进行20%的随机抽样
 * @author myc
 * @version 1.0
 */
public class Sampling {

    /**
     * Mapper类：从data.txt中读取数据，并按career字段进行分组
     * 提取职业字段作为key，整行数据作为value输出
     */
    public static class SamplingMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * 处理每一行输入数据，将其分解成字段，按照career进行分组
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文对象
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 按照'|'分割每行记录，提取各个字段
            String[] fields = value.toString().split("\\|");
            // career是第11个字段，确保存在career字段
            if(fields.length > 10) {
                String career = fields[10]; // 提取career字段
                // 构造<career, 整条记录>键值对，输出给Reducer
                context.write(new Text(career), value);
            }
        }
    }

    /**
     * Reducer类：对每个分组进行抽样
     * 从每组中随机抽取20%的记录，并将结果输出
     */
    public static class SamplingReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 处理Mapper阶段分组后的数据，对每组进行随机抽样
         *
         * @param key Mapper输入的career字段
         * @param values 对应职业的记录
         * @param context 上下文对象，用于输出抽样后的记录
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // records存储当前career字段对应的所有记录
            List<Text> records = new ArrayList<Text>();
            for(Text value : values) {
                // 将所有记录添加到列表中
                records.add(new Text(value));
            }
            // 抽样概率为20%
            double sampleRate = 0.2;
            // 计算抽样数量
            int sampleSize = (int)(records.size()*sampleRate);
            // 进行随机抽样
            Random random = new Random();
            for(int i = 0; i < sampleSize; i++) {
                // 随机抽取一个记录索引
                int index = random.nextInt(records.size());
                // 输出该索引对应的记录
                context.write(records.get(index), new Text(""));
                // 删除该记录，防止重复抽样
                records.remove(index);
            }
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
        String inputPath = "hdfs://myc-ubuntu:9000/Lab1/data.txt";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Sample";
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
        Job job = Job.getInstance(conf, "Sample");
        // 指定Mapper类和Reducer类
        job.setMapperClass(SamplingMapper.class);
        job.setReducerClass(SamplingReducer.class);
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
