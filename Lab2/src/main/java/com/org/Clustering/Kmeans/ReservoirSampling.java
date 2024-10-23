package com.org.Clustering.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ReservoirSampling类：抽取k个记录作为初始的中心点
 */
public class ReservoirSampling {
    private static int k = 1225;    // 要抽取的记录个数

    /**
     * ReservoirMapper类：使用水库抽样的方法抽取k个记录
     */
    public static class ReservoirSamplingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String[] centroids;
        private int count = 0;  // 记录序号
        private Random rand = new Random(); // 初始化随机


        /**
         * 初始化字符串数组centroids
         *
         * @param context MapReduce上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce中断可能出现的异常
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            centroids = new String[k];
        }

        /**
         * 使用水库抽样的方法抽取k个样本，结果保存在centroids中
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能抛出的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(count < k)
            {
                // 水库未满，这个记录直接加入到水库中
                centroids[count] = value.toString();
            }else
            {
                // 水库已满，以k/count的概率保留记录，然后以1/k的概率淘汰当前的k个抽样结果
                int randomIndex = rand.nextInt(count);  // 在[0, count - 1]中生成一个随机数
                if(randomIndex < k)
                {
                    centroids[randomIndex] = value.toString();  // 保留这个记录
                }
            }
            count++;
        }

        /**
         * 将抽样结果centroids输出到上下文
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(String centroid : centroids)
            {
                context.write(new Text(centroid), new Text(""));    // 以抽样的记录为键，以空字符串为值
            }
        }
    }

    /**
     * ReservoirSamplingReducer类：接收多个Mapper的抽样结果，并从中抽取出k个记录作为最终的抽样结果
     */
    public static class ReservoirSamplingReducer extends Reducer<Text, Text, Text, Text> {
        private String[] centroids;
        private int count = 0;
        private Random rand = new Random();

        /**
         * 初始化字符串数组
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            centroids = new String[k];
        }
        /**
         * 使用水库抽样的方法抽取出k个记录
         *
         * @param key Mapper输出的记录
         * @param values 空字符串
         * @param context MapReduce上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常:w
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();

            if(count < k)
            {
                centroids[count] = line;
            }else
            {
                int randomIndex = rand.nextInt(count);
                if(randomIndex < k)
                {
                    centroids[randomIndex] = line;
                }
            }
            count++;
        }

        /**
         * 将最终的抽样结果输出到HDFS中
         *
         * @param context MapReduce上下文
         * @throws IOException 文件IO可能排除的异常
         * @throws InterruptedException MapReduce中断抛出的异常
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(String centroid : centroids)
            {
                context.write(new Text(centroid), new Text(""));
            }
        }
    }

    /**
     * MapReduce的入口方法，配置作业参数并启动任务
     *
     * @param args 命令行参数
     * @throws Exception MapReduce作业启动时抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // 定义文件输入输出路径
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/cluster/raw_data";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/cluster/centroids";
        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 设置默认文件系统为HDFS
        // 获取HDFS文件系统对象
        FileSystem fs = FileSystem.get(conf);
        // 删除上一次的作业结果（如果存在）
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir))
        {
            fs.delete(outputDir, true);
        }
        // 创建一个MapReduce作业
        Job job = Job.getInstance(conf, "ReservoirSampling");
        // 指定Mapper类和Reducer类
        job.setMapperClass(ReservoirSampling.ReservoirSamplingMapper.class);
        job.setReducerClass(ReservoirSampling.ReservoirSamplingReducer.class);
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
