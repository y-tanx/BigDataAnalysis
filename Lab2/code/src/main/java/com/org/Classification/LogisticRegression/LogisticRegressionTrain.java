package com.org.Classification.LogisticRegression;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * LogisticRegressionTrain类：使用逻辑回归方法训练模型
 */
public class LogisticRegressionTrain {
    private static int row = 20;
    private static double learningRate = 0.00001;   // 学习率
    private static int iterations = 6000;   // 迭代次数

    public static class LRMapper extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String outputKey = "sample";
            // 直接将样本传递给Reducer，而且保证所有的样本都传递给同一个Redcuer
            context.write(new Text(outputKey), value);
        }
    }

    public static class LRReducer extends Reducer<Text, Text, Text, Text> {
        private double[] weights; // 20个权重

        protected void setup(Context context) throws IOException, InterruptedException {
            Random rand = new Random();
            weights = new double[row];
            for (int i = 0; i < row; i++) {
                weights[i] = rand.nextDouble() * 0.02 - 0.01; // 初始化权重为[-0.01, 0.01]之间的随机值
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            // 将样本存储到列表中
            List<double[]> samples = new ArrayList<>();
            List<Double> labels = new ArrayList<>();

            for(Text value : values)
            {
                String[] fields = value.toString().split(",");
                double[] trainSample = new double[row];
                for (int i = 0; i < row; ++i) {
                    trainSample[i] = Double.parseDouble(fields[i]);
                }
                double label = Double.parseDouble(fields[row]);

                // 将样本和标签添加到列表中
                samples.add(trainSample);
                labels.add(label);
            }

            for(int iter = 0; iter < iterations; ++iter)
            {
                double[] gradient = new double[row];    // 梯度数组
                int sampleCount = samples.size();    // 样本计数器
                Arrays.fill(gradient, 0.0);

                // 进行一轮迭代
                for(int i = 0; i < sampleCount; ++i)
                {
                    double[] trainSample = samples.get(i);
                    double label = labels.get(i);

                    // 计算预测值
                    double predicted = PredictClass(trainSample);
                    double error = predicted - label;   // 计算误差

                    // 累加梯度
                    for(int j = 0; j < row; ++j)
                    {
                        gradient[j] += error * trainSample[j];
                    }
                }

                // 一次迭代结束，更新权重
                for(int i = 0; i < row; ++i)
                {
                    weights[i] -= (learningRate * (gradient[i] / sampleCount));
                }
            }
            // 迭代结束，输出权重数组
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < row; ++i)
            {
                sb.append(weights[i]);
                if (i < row - 1) sb.append(",");
            }
            sb.append("\n");
            context.write(new Text(sb.toString()), new Text(""));
        }

        private double PredictClass(double[] trainSample){
            double result = 0.0;

            // 计算点积
            double dotProduct = 0.0;
            for(int i = 0; i < row; ++i)
            {
                dotProduct += weights[i] * trainSample[i];
            }

            // 代入到sigmoid函数
            result = 1 / (1 + Math.exp(-dotProduct));
            return result;
        }
    }

    /**
     * MapReduce入口方法，执行MapReduce任务
     *
     * @param args 命令行参数
     * @throws Exception MapReduce执行过程中抛出的异常
     */
    public static void main(String[] args) throws Exception {
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/train";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/LR/weight";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 默认文件系统为HDFS

        // 删除上一次的作业
        FileSystem fs = FileSystem.get(conf);
        Path ouputDir = new Path(outputPath);
        if(fs.exists(ouputDir))
        {
            fs.delete(ouputDir);
        }
        fs.close();

        // 创建MapReduce作业
        Job job = Job.getInstance(conf, "LR Train");
        // 指定Mapper类和Reducer类
        job.setMapperClass(LogisticRegressionTrain.LRMapper.class);
        job.setReducerClass(LogisticRegressionTrain.LRReducer.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
