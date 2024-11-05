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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * LogsticRegressionValidation类：验证逻辑回归模型的准确率
 */
public class LogisticRegressionValidation {
    private static int row = 20;
    private static double SampleNum = 300000;

    public static class LRMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);  // 直接输出到reduce
        }
    }

    public static class LRReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private double[] weights;
        private long lineNumber = 1;
        private double rightPrediction = 0.0;

        protected void setup(Context context) throws IOException, InterruptedException {
            weights = new double[row];
            // 从文件中读取权重数组
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path weightPath = new Path("hdfs://myc-ubuntu:9000/Lab2/classification/LR/weight/part-r-00000");

            // 读取第一行的数据
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(weightPath))))
            {
                String firstLine = br.readLine();
                if(firstLine == null)
                {
                   throw new RuntimeException("no data!");
                }else
                {
                    String[] fields = firstLine.split(",");
                    for(int i = 0; i < row; ++i)
                    {
                        weights[i] = Double.parseDouble(fields[i]);
                    }
                }
            }catch (Exception e)
            {
                e.printStackTrace();
                throw new RuntimeException("读取权重文件失败!");
            }
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 预测所属类别，并与标签进行比较，输出错误的类别，计算准确率
            Iterator<Text> iter = values.iterator();
            String line = iter.next().toString();
            String[] fields = line.split(",");
            double[] validationSample = new double[row];
            for(int i = 0; i < row; i++) {
                validationSample[i] = Double.parseDouble(fields[i]);
            }
            int label = Integer.parseInt(fields[row]);

            // 预测所属类别
            double predictedProbability = PredictClass(validationSample);
            int predictedClass = (predictedProbability > 0.5) ? 1 : 0;

            // 输出预测错误的记录
            if(label != predictedClass)
            {
                context.write(new LongWritable(lineNumber), new Text(line));
            }else
            {
                rightPrediction++;
            }
            lineNumber++;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            double rightProb = rightPrediction / SampleNum;
            context.write(new LongWritable(300000), new Text(String.valueOf(rightProb)));
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
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/validation";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/LR/validation_Done";

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
        Job job = Job.getInstance(conf, "LR Validation");
        // 指定Mapper类和Reducer类
        job.setMapperClass(LogisticRegressionValidation.LRMapper.class);
        job.setReducerClass(LogisticRegressionValidation.LRReducer.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
