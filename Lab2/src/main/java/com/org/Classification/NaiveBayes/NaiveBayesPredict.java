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
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * NaiveBayesPredict类：使用贝叶斯分类器对测试数据集进行分类
 */
public class NaiveBayesPredict {
    private static int classNum = 2;
    private static int attributeNum = 20;
    private static int conditionNum = 2;

    public static class NaiveBayesMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private int trainRecordNum = 900000;    // 训练数据集的记录数量
        private int[] classStats;
        private double[] priorProbs;     // 先验概率
        private double[][][] conditionProbs;    // 条件概率

        /**
         * 读取训练结果，计算先验概率和条件概率
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            classStats = new int[classNum]; // 记录各个类别中包含的记录数
            priorProbs = getPriorProb(context, classStats);  // 从"class_stats"中读取先验概率
            conditionProbs = getConditionProb(context, classStats); // 从"attribute_stats"中读取条件概率
        }

        /**
         * 使用训练获得的先验概率和条件概率对记录进行分类，并与记录的标签进行比较
         *
         * @param key 当前文本行的字节偏移量
         * @param value 当前的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            double[] record = new double[attributeNum]; // 20个属性
            for(int i = 0; i < attributeNum; i++) {
                record[i] = Double.parseDouble(fields[i]);
            }

            // 使用贝叶斯分类器对记录进行分类，输出<记录，类别>
            int predictClass = NaiveBayesPredict(record);
            context.write(key, new IntWritable(predictClass));
        }

        /**
         * 从“class_stats”文件中读取先验概率
         *
         * @param context MapReduce的上下文
         * @param classStats 记录各个类别中的记录数量
         * @return double[] 先验概率
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        private double[] getPriorProb(Context context, int[] classStats) throws IOException, InterruptedException {
            double[] priorProbs = new double[classNum];

            // 获得HDFS文件系统对象
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            // 读取文件"class_stats-r-00000"
            String priorPath = "hdfs://myc-ubuntu:9000/Lab2/classification/train_Done/class_stats-r-00000";
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(priorPath))))) {
                String line;
                while( (line = br.readLine()) != null ) {   // 读取一行
                    String[] fields = line.split("\t");
                    int classID = Integer.parseInt(fields[0]);  // 类别序号
                    int classRecordNum = Integer.parseInt(fields[1]); // 类别中包含的记录数量
                    classStats[classID] = classRecordNum;   // 保存类别classID包含的记录数量
                    priorProbs[classID] = classRecordNum / (trainRecordNum * 1.0);    // 计算类别classID的先验概率
                }
            }catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("读取先验概率文件失败!");
            }
            return priorProbs;
        }

        /**
         * 从"attribute_stats"文件中读取条件概率
         *
         * @param context MapReduce的上下文
         * @param classStats 各个类别中包含的记录数，用于计算条件概率
         * @return double[][][] 条件概率
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        private double[][][] getConditionProb(Context context, int[] classStats) throws IOException, InterruptedException {
            double[][][] conditionProbs = new double[classNum][attributeNum][conditionNum];

            // 获得HDFS文件对象
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            // 读取文件"attribute_stats-r-00000"
            String conditionPath = "hdfs://myc-ubuntu:9000/Lab2/classification/train_Done/attribute_stats-r-00000";
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(conditionPath))))){
                String line;
                while( (line = br.readLine()) != null ) {
                    String[] fields = line.split("\t");
                    int classID = Integer.parseInt(fields[0]);  // 类别序号
                    int attributeID = Integer.parseInt(fields[1]); // 属性序号
                    int condition = Integer.parseInt(fields[2]); // 属性值的正负情况，1代表正，0代表负
                    int conditionStats = Integer.parseInt(fields[3]);
                    conditionProbs[classID][attributeID][condition] = conditionStats / (classStats[classID] * 1.0);   // 计算条件概率
                }
            }catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("读取条件概率文件失败!");
            }
            return conditionProbs;
        }

        /**
         * 使用朴素贝叶斯分类器对记录进行分类
         *
         * @param record 记录的字段数组
         * @return int 记录所属的类别
         */
        private int NaiveBayesPredict(double[] record) {
            double[] classificationProbs = new double[classNum]; // 记录属于各个类别的概率

            // 计算记录属于每个类别的概率
            for(int i = 0; i < classNum; i++) {
                double priorProb = priorProbs[i];   // 先验概率
                double conditionProb = 1;
                for(int j = 0; j < attributeNum; j++) {
                    int condition = (record[j] > 0) ? 1 : 0;    // 1为正，0为负
                    conditionProb *= conditionProbs[i][j][condition];
                }
                classificationProbs[i] = conditionProb * priorProb; // 保存记录属于类别i的概率，忽略公共项分母
            }

            double maxProb = 0;
            int maxIndex = -1;
            // 寻找最大概率
            for(int i = 0; i < classNum; ++i)
            {
                // 更新最大概率和对应的类别序号
                if(classificationProbs[i] > maxProb){
                    maxProb = classificationProbs[i];
                    maxIndex = i;
                }
            }
            if(maxIndex == -1){
                throw new RuntimeException("预测分类失败!");
            }
            return maxIndex;
        }
    }

    public static class NaiveBayesReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
        private long lineNumber = 1;

        /**
         * 统计预测失败的记录数量，输出预测失败的记录
         *
         * @param key 预测失败的记录
         * @param values 预测失败的记录数量
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

            int predictClass = -1;
            Iterator<IntWritable> iter = values.iterator();
            if(iter.hasNext()){
                predictClass = iter.next().get();
            }
            if(predictClass == -1){
                throw new RuntimeException("输出分类结果失败!");
            }
            // 输出<行号，分类结果>
            context.write(new LongWritable(lineNumber), new IntWritable(predictClass));
            lineNumber++;
        }
    }

    public static void main(String[] args) throws Exception{
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/test";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/classification/test_Done";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000");

        // 删除上一次的作业
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir)) {
            fs.delete(outputDir);
        }
        fs.close();

        // 创建MapReduce作业
        Job job = Job.getInstance(conf, "Naive Bayes Test");
        // 指定Mapper类和Reducer类
        job.setMapperClass(NaiveBayesPredict.NaiveBayesMapper.class);
        job.setReducerClass(NaiveBayesPredict.NaiveBayesReducer.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}