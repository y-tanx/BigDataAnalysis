package com.org.Filter;

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

/**
 * 该类对属性longitude和latitude进行奇异值过滤
 * longtitude有效范围：[8.1461259, 11.1993265]
 * latitude有效范围：[56.5824856, 57.750511]
 * @author myc
 * @version 1.0
 */
public class Filter {

    /**
     * Mapper类：从D_Sample中读取记录，根据字段longtitude和latitude进行筛选
     * 以两个字段均在有效范围内的记录为key，空字符串为value
     */
    public static class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * 处理每一行数据，将其分割为字段，根据字段longtitude和latitude的有效范围进行筛选
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");
            // longtitude和latitude是第2个和第3个属性
            double longtitude = Double.parseDouble(fields[1]);
            double latitude = Double.parseDouble(fields[2]);
            // 检验这个文本行的两个字段是否位于正确范围内，如果是，则输出到上下文中
            if(longtitude >= 8.1461259 && longtitude <= 11.1993265 && latitude >= 56.5924956 && latitude <= 57.750511)
            {
                context.write(value, new Text(""));
            }
        }
    }
    /**
     * Reducer类：Shuffle阶段会按照key进行分组，相同的key会合并在一起,如果使用默认的Reducer，则不会去重。为了实现去重，要自定义Reducer类
     *
     */
    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 直接输出Mapper的键值对，用来去重
         *
         * @param key Mapper输入的career字段
         * @param values 对应职业的记录
         * @param context 上下文对象，用于输出抽样后的记录
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
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
        String inputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Sample/part-r-00000";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Filter";
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
        Job job = Job.getInstance(conf, "Filter");
        // 指定Mapper类，Reducer类
        job.setMapperClass(Filter.FilterMapper.class);
        job.setReducerClass(Filter.FilterReducer.class);
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
