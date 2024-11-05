package com.org.Clustering.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class KMeans {
    private static double delta = 20.0;
    private static int row = 20;
    private static boolean needLoop = true;

    /**
     * KMeansMapper类：重新划分类别，输出每个记录和它所属于的类别序号
     */
    public static class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, RecordClusteridWritable> {
        private List<double[]> centroids;

        /**
         * 读取广播的中心点数据，初始化列表centroids
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能抛出的异常
         * @throws InterruptedException MapReduce中断时抛出的异常
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化中心点列表
            Configuration conf = context.getConfiguration();
            centroids = new ArrayList<>();

            // 读取广播的中心点数据
            for(int i = 0; ; ++i)
            {
                String serializedCentroid = conf.get("centroid." + i);
                if(serializedCentroid == null)
                {
                    break;
                }
                double[] centroid = deserializeCentroid(serializedCentroid);
                centroids.add(centroid);    // 加入类别i的中心点
            }
        }

        /**
         * 计算记录与各个中心点的距离，将记录划分到距离最小的类别
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 构造记录的字段数组
            String line = value.toString();
            String[] fields = line.split(",");
            double[] record = new double[row];  // 记录的字段数组
            for(int i = 0; i < row; ++i)
            {
                record[i] = Double.parseDouble(fields[i]);
            }

            // 计算记录与各个中心点的距离，将记录划分到距离最小的类别中
            int clusterID = getClosetCentroid(record);
            if(clusterID == -1)
            {
                throw new RuntimeException("CLuster ID not found");
            }
            // 输出<记录，记录所属类别>键值对
            context.write(key, new RecordClusteridWritable(line, clusterID));
        }

        /**
         * 计算记录与各个中心点之间的距离，判断该记录所属的类别
         *
         * @param record 记录字段数据
         * @return int 该记录所属的类别
         */
        private int getClosetCentroid(double[] record)
        {
            int size = centroids.size();    // 中心点个数
            double minDistance = Double.MAX_VALUE;      // 最小距离
            int clusterID = -1;             // 最小距离对应的中心点序号

            // 遍历各个中心点，计算与该记录的距离，获得最小距离对应的中心点序号
            for(int i = 0; i < size; ++i)
            {
                double[] centroid = centroids.get(i);   // 获得第i个中心点
                double distance = 0;

                // 计算记录record与中心点i的距离
                for(int j = 0; j < row; ++j)
                {
                    distance += Math.pow(centroid[j] - record[j], 2);
                }

                // 检验并更新最小距离
                if(distance < minDistance)
                {
                    // 当前距离是最小距离，更新minDistance和clusterID
                    minDistance = distance;
                    clusterID = i;
                }
            }
            return clusterID;
        }
    }

    public static class KMeansReducer extends Reducer<LongWritable, RecordClusteridWritable, LongWritable, IntWritable> {
        private List<double[]> centroids;
        private HashMap<Integer, ClusterSumNum> clusterMap;
        private long lineNumber = 1;

        /**
         * 读取广播的中心点数据，初始化列表centroids和哈希表clusterMap
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            centroids = new ArrayList<>();

            // 读取广播的中心点数据
            for(int i = 0; ; ++i)
            {
                String serializedCentroid = conf.get("centroid." + i);
                if(serializedCentroid == null)
                {
                    break;
                }
                double[] centroid = deserializeCentroid(serializedCentroid);
                centroids.add(centroid);
            }

            // 初始化哈希表
            clusterMap = new HashMap<>();
        }

        /**
         * 更新各个类别的记录数与列和，输出聚类结果
         *
         * @param key 记录
         * @param values 记录所属的类别
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void reduce(LongWritable key, Iterable<RecordClusteridWritable> values, Context context) throws IOException, InterruptedException {
            // 获取RecordClusteridWritable对象
            for(RecordClusteridWritable recordClusterid : values){
                String line = recordClusterid.getRecord();
                int clusterID = recordClusterid.getClusterID();
                // 获得记录的字段数组
                String[] fields = line.toString().split(",");
                double[] record = new double[row];
                for(int i = 0; i < row; ++i)
                {
                    record[i] = Double.parseDouble(fields[i]);
                }

                // 更新记录所属类别的记录数和列和
                ClusterSumNum clusterSumNum = clusterMap.getOrDefault(clusterID, new ClusterSumNum(0, new double[row]));
                ClusterSumNum addSumNum = new ClusterSumNum(1, record);
                clusterSumNum.merge(addSumNum);

                // 更新哈希表
                clusterMap.put(clusterID, clusterSumNum);

                // 输出<行号，所属类别>
                context.write(new LongWritable(lineNumber), new IntWritable(clusterID));
                lineNumber++;
            }
        }

        /**
         * 根据哈希表的表项更新中心点，计算更新前后中心点的距离，判断是否要继续循环更新中心点
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<double[]> newCentroids = new ArrayList<>();    // 存放更新后的中心点的列表
            double centroidsDistance = 0.0;

            // 按序遍历哈希表，计算各个类别的中心点
            List<Integer> sortedKeys = new ArrayList<>(clusterMap.keySet());
            Collections.sort(sortedKeys);
            for(Integer clusterID : sortedKeys){
                // 获得类别clusterID的记录数与列和
                ClusterSumNum clusterSumNum = clusterMap.get(clusterID);
                double[] clusterSum = clusterSumNum.getSum();
                int clusterNum = clusterSumNum.getNum();

                // 计算类别clusterID的中心点
                double[] newCentroid = new double[row];
                for(int i = 0; i < row; ++i)
                {
                    newCentroid[i] = clusterSum[i] / clusterNum;
                }

                // 加入到中心点列表
                newCentroids.add(newCentroid);
            }

            // 将更新后的中心点列表写到中心点文件
            WriteCentroids(context, newCentroids);

            // 计算更新前后中心点的距离
            int size = newCentroids.size();
            for(int i = 0; i < size; ++i)
            {
                double[] oldCentroid = centroids.get(i);
                double[] newCentroid = newCentroids.get(i);
                double distance = 0;
                for(int j = 0; j < row; ++j)
                {
                    distance += Math.pow(newCentroid[j] - oldCentroid[j], 2);
                }
                centroidsDistance += Math.sqrt(distance);
            }
            System.out.println("\n");
            System.out.println("====================");
            System.out.println("Distance");
            System.out.println(centroidsDistance);
            System.out.println("=====================");
            System.out.println("\n");
            Thread.sleep(1000);

            // 距离与delta进行比较，判断是否需要继续循环更新中心点
            if(centroidsDistance < delta)
            {
                needLoop = false;
            }
        }

        /**
         * 将更新后的中心点写入到中心点文件
         *
         * @param context MapReduce的上下文，包含了HDFS文件系统对象
         * @param newCentorids 更新后的中心点列表
         * @throws IOException 文件IO可能抛出的异常
         */
        private void WriteCentroids(Context context, List<double[]> newCentorids) throws IOException
        {
            Path centroidsPath = new Path("hdfs://myc-ubuntu:9000/Lab2/cluster/centroids/part-r-00000");

            // 获取HDFS文件系统对象
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            // 删除上一次的中心点文件
            if(fs.exists(centroidsPath))
            {
                fs.delete(centroidsPath, true);
            }

            // 创建输出流
            try(FSDataOutputStream out = fs.create(centroidsPath))
            {
                // 将每个中心点以逗号分隔写入文件
                for (double[] newCentroid : newCentorids) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < row; i++) {
                        sb.append(newCentroid[i]);
                        if (i < row - 1) {
                            sb.append(","); // 添加逗号分隔符
                        }
                    }
                    out.writeBytes(sb.toString() + "\n"); // 写入到文件并换行
                }
            }catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("中心点文件写入失败!");
            }
        }
    }

    /**
     * 从中心点文件中读取并序列化每个中心点记录
     *
     * @param conf Hadoop作业的配置信息
     * @throws IOException 文件IO可能抛出的异常
     */
    private static void serializeCentroids(Configuration conf) throws IOException{
        Path centroidsPath = new Path("hdfs://myc-ubuntu:9000/Lab2/cluster/centroids/part-r-00000");    // 中心点的文件路径
        FileSystem fs = FileSystem.get(conf);   // 获得HDFS文件系统对象

        int count = 0;
        // 读取每个记录，并序列化存放到conf
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath))))
        {
            // 读取一行记录
            String line;
            while((line = br.readLine()) != null)
            {
                conf.set("centroid." + count, line);
                count++;
            }
        }catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException("中心点文件序列化失败!");
        }
    }

    /**
     * 反序列化中心点，构造中心点字段数组
     *
     * @param serializedCentroid 序列化的中心点字符串
     * @return double[] 构造的中心点字段数组
     */
    private static double[] deserializeCentroid(String serializedCentroid)
    {
        String[] fields = serializedCentroid.split(",");
        double[] centroid = new double[row];
        for(int i = 0; i < row; ++i)
        {
            centroid[i] = Double.parseDouble(fields[i]); // 构造中心点字段数组
        }
        return centroid;
    }

    /**
     * MapReduce的入口方法，配置作业信息
     *
     * @param args 命令行参数
     * @throws Exception MapReduce作业启动时抛出的异常
     */
    public static void main(String[] args) throws Exception{
        // 定义文件输入输出路径
        String inputPath = "hdfs://myc-ubuntu:9000/Lab2/cluster/raw_data";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/cluster/KMeans_Done";
        while(needLoop)
        {
            // 创建Hadoop配置对象
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 设置默认文件系统为HDFS
            serializeCentroids(conf);   // 读取并序列化中心点列表
            // 获取HDFS文件系统对象
            FileSystem fs = FileSystem.get(conf);

            // 删除上一次的作业
            Path outputDir = new Path(outputPath);
            if(fs.exists(outputDir))
            {
                fs.delete(outputDir, true);
            }
            fs.close();

            // 创建MapReduce作业
            Job job = Job.getInstance(conf, "KMeans");  // 创建一个名为"KMeans"的作业实例

            // 指定Mapper类和Reducer类
            job.setMapperClass(KMeans.KMeansMapper.class);
            job.setReducerClass(KMeans.KMeansReducer.class);

            // 指定maptask输出类型
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(RecordClusteridWritable.class);

            // 指定reducetask输出类型
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(IntWritable.class);

            // 设置输入和输出路径
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            // 提交任务
            boolean success = job.waitForCompletion(true);
            if(!success)
            {
                throw new RuntimeException("任务执行失败!");
            }
        }
        System.exit(0);
    }
}