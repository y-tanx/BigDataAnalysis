package com.org.Classification.KNN;

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
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.PriorityQueue;

/**
 * KNNPredict类：使用KNN算法对测试样本集进行分类
 */
public class KNNPredict {
    private static int row = 20;
    private static int classNum = 2;
    private static int K = 20;

    /**
     * KNNpredictMapper类：遍历训练集样本，计算各个测试样本到不同类别之间的距离
     */
    public static class KNNPredictMapper extends Mapper<LongWritable, Text, IntWritable, ClassDistanceWritable> {

        /**
         * 遍历训练样本集，计算测试样本到每一个训练样本的距离
         *
         * @param key 当前行的偏移量
         * @param value 当前文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取训练样本的字段数组
            String[] fields = value.toString().split(",");
            double[] trainSample = new double[row];
            for(int j = 0; j < row; j++) {
                trainSample[j] = Double.parseDouble(fields[j]);
            }
            int classID = Integer.parseInt(fields[row]);    // 训练样本的类别ID
            Configuration conf = context.getConfiguration();

            // 遍历测试样本集，计算各个测试样本到该训练样本的距离
            for(int i = 0; ; ++i)
            {
                String testSampleLine = conf.get("test." + i);
                if(testSampleLine == null)
                {
                    break;
                }
                double[] testSample = getTestSample(testSampleLine);
                double distance = 0;
                // 计算测试样本i到训练样本的距离
                for(int j = 0; j < row; j++) {
                    distance += Math.pow(testSample[j] - trainSample[j], 2);
                }
                distance = Math.sqrt(distance);

                // 输出键值对：<测试样本ID，(训练样本的类别ID，测试样本到该训练样本的距离)>
                context.write(new IntWritable(i), new ClassDistanceWritable(classID, distance));
            }
        }

        private double[] getTestSample(String testSampleLine) {
            double[] testSample = new double[row];
            String[] fields = testSampleLine.split(",");
            for(int j = 0; j < row; j++) {
                testSample[j] = Double.parseDouble(fields[j]);
            }
            return testSample;
        }
    }

    /**
     * KNNPredictReducer类：统计测试样本的前k个最小的距离，对测试样本进行分类
     */
    public static class KNNPredictReducer extends Reducer<IntWritable, ClassDistanceWritable, IntWritable, IntWritable>{

        /**
         * 统计测试样本的前k个最小距离，以出现次数最多的类别ID作为测试样本的类别
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(IntWritable key, Iterable<ClassDistanceWritable>values, Context context) throws IOException, InterruptedException {
            // 维护一个最大堆
            PriorityQueue<ClassDistanceWritable> knnQueue = new PriorityQueue<>(K, (a, b) ->{
                double distanceA = a.getDistance();
                double distanceB = b.getDistance();
                return Double.compare(distanceB, distanceA);    // 距离更大的元素放在队列的顶部
            });

            // 将所有样本按照距离排序，选出前K个最近的邻居
            for(ClassDistanceWritable value : values)
            {
                knnQueue.add(value);
                if(knnQueue.size() > K)
                {
                    knnQueue.poll();    // 删除距离最大的样本，维持队列的大小为K
                }
            }

            // 在最近的K个邻居中统计邻居类别的个数
            int[] classStats = new int[classNum];
            for(ClassDistanceWritable neighbor : knnQueue)
            {
                int classID = neighbor.getClassID();
                classStats[classID]++;
            }

            // 选出出现次数最多的类别ID，它是该测试样本的所属类别
            int max = classStats[0];
            int testClassID = 0;
            for(int i = 1; i < classStats.length; ++i)
            {
                if(classStats[i] > max)
                {
                    max = classStats[i];
                    testClassID = i;
                }
            }

            // 输出<样本序号，样本所属的类别>
            context.write(key, new IntWritable(testClassID));
        }
    }

/**
 * 从测试数据集中读取数据，存放在conf中
 *
 * @param conf Hadoop作业的配置信息
 * @throws IOException 文件IO可能抛出的异常
 */
private static void readTestSamples(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path testPath = new Path("hdfs://myc-ubuntu:9000/Lab2/classification/test");

    // 读取文件
    int count = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(testPath)))) {
        // 读取一行记录
        String line;
        while ((line = br.readLine()) != null) {
            conf.set("test." + count, line);
            count++;
        }
    } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("测试集文件读取失败!");
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
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/KNN/test_Done";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 默认文件系统为HDFS
        readTestSamples(conf);  // 将测试集保存在conf中

        // 删除上一次的作业
        FileSystem fs = FileSystem.get(conf);
        Path ouputDir = new Path(outputPath);
        if(fs.exists(ouputDir))
        {
            fs.delete(ouputDir);
        }
        fs.close();

        // 创建MapReduce作业
        Job job = Job.getInstance(conf, "KNN Test");
        // 指定Mapper类和Reducer类
        job.setMapperClass(KNNPredict.KNNPredictMapper.class);
        job.setReducerClass(KNNPredict.KNNPredictReducer.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ClassDistanceWritable.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
