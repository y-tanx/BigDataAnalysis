package com.org.Clustering.KMedoide;

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
import java.util.ArrayList;
import java.util.List;

/**
 * KMedoide类：使用K中心点算法实现聚类
 */
public class KMedoide {
    private static int k = 1225;    // 要划分的类别个数
    private static int row = 20;    // 每一个记录的维度
    private static double delta = 5.0;
    private static boolean needLoop = true;

    /**
     * KMedoideMapper类：重新划分类别。输出类别序号和类别中包含的记录
     */
    public static class KMedoideMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        List<double[]> centroids;

        /**
         * 读取广播的数据，初始化列表centroids
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化中心点列表
            Configuration conf = context.getConfiguration();
            centroids = new ArrayList<>();

            // 读取广播的中心点数据
            for(int i = 0; ; ++i)
            {
                String serializedCentroid = conf.get("centroid." + i);
                if(serializedCentroid == null){
                    break;
                }
                double[] centroid = deserializeCentroid(serializedCentroid);
                centroids.add(centroid);
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
            // 构造记录的字段数据
            String line = value.toString();
            String[] fields = line.split(",");
            double[] record = new double[row];
            for(int i = 0; i < row; ++i)
            {
                record[i] = Double.parseDouble(fields[i]);
            }

            // 计算记录与各个中心点之间的距离，将记录划分到距离最小的类别中
            int clusterID = getClosetCentroid(record);
            if(clusterID == -1)
            {
                throw new RuntimeException("Cluster ID not found");
            }

            // 输出<类别序号，记录>
            context.write(new IntWritable(clusterID), new Text(line));
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

    /**
     * KMedoideReducer类：更新中心点，计算总体相异度，判断是否需要继续循环
     */
    public static class KMedoideReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        List<double[]> centroids;
        List<double[]> newCentroids;

        /**
         * 初始化原中心点列表和新中心点列表，从广播中读取原中心点
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
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
                centroids.add(centroid);
            }

            // 初始化新中心点列表
            newCentroids = new ArrayList<>(k);
        }

        /**
         * 更新各个类别的中心点和类别相异度，输出<类别序号，记录>
         *
         * @param key 类别序号
         * @param values 类别中包含的记录
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int clusterID = key.get();  // 类别序号
            List<double[]> clusterRecords = new ArrayList<>();

            // 将类别clusterID中的所有记录保存在记录列表中，同时输出<类别序号，类别包含的记录>
            for(Text value : values)
            {
                String line = value.toString();
                String[] fields = line.split(",");
                double[] record = new double[row];
                for(int i = 0; i < row; ++i)
                {
                    record[i] = Double.parseDouble(fields[i]);
                }
                clusterRecords.add(record);

                // 输出<类别序号，类别包含的记录>
                context.write(new IntWritable(clusterID), new Text(line));
            }

            // 更新中心点，以类别中与其他点距离之和最小的点作为新的中心点
            double[] newCentroid = getNewCentroid(clusterRecords);
            newCentroids.add(clusterID, newCentroid);   // 将类别clusterID的新中心点加入到列表中
        }

        /**
         * 将更新后的中心点写到中心点文件，通过比较总体相异度与delta判断是否要继续循环
         *
         * @param context MapReduce的上下文
         * @throws IOException 文件IO可能抛出的异常
         * @throws InterruptedException MapReduce任务中断抛出的异常
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // 计算更新前后中心点之间的距离
            double centroidsDistance = 0.0;
            int size = newCentroids.size();

            for(int i = 0; i < size; ++i)
            {
                double[] oldCentroid = centroids.get(i);
                double[] newCentroid = newCentroids.get(i);
                double distance = 0.0;

                for(int j = 0; j < row; ++j)
                {
                    distance += Math.pow(oldCentroid[j] - newCentroid[j], 2);
                }
                centroidsDistance += Math.sqrt(distance);
            }

            // 输出
            System.out.println("\n");
            System.out.println("====================");
            System.out.println(centroidsDistance);
            System.out.println("====================");
            System.out.println("\n");
            Thread.sleep(1000);

            if(centroidsDistance < delta)
            {
                needLoop = false;
            }

            // 将更新后的中心点写回到文件中
            WriteCentroids(context, newCentroids);
        }

        /**
         * 计算类别中各个点与其他点的距离之和，更新类别的中心点和类别的相异度
         *
         * @param clusterRecords 该类别包含的记录的列表
         * @return double[] 该类别的新中心点
         */
        private double[] getNewCentroid(List<double[]> clusterRecords){
            int num = clusterRecords.size(); // 记录的个数
            double minDistance = Double.MAX_VALUE; // 记录最小的距离和
            int recordID = -1;

            // 对于类别中的每个记录点，计算它与其他记录点之间的距离之和，并维持最小距离和 与 对应的记录点的序号
            for(int i = 0; i < num; ++i){
                double[] record = clusterRecords.get(i);
                double distanceSum = 0.0;

                for(int j = 0; j < num; ++j){
                    double[] otherRecord = clusterRecords.get(j);
                    double distance = 0.0;

                    for(int k = 0; k < row; ++k){
                        distance += Math.pow(record[k] - otherRecord[k], 2);
                    }
                    distanceSum += Math.sqrt(distance);
                }

                // 更新最小距离和 与 对应的记录序号
                if(distanceSum < minDistance){
                    minDistance = distanceSum;
                    recordID = i;
                }
            }

            // 更新中心点，返回该类别的相异度
            if(recordID == -1){
                throw new RuntimeException("中心点更新失败!");
            }
            double[] newCentroid = clusterRecords.get(recordID);

            return newCentroid;
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
     * 反序列化中心点，构造中心点字段数据
     *
     * @param serializedCentroid 序列化的中心点字符串
     * @return double[] 构造的中心点字段数组
     */
    private static double[] deserializeCentroid(String serializedCentroid){
        double[] centroid = new double[row];
        String[] fields = serializedCentroid.split(",");
        for(int i = 0; i < row; ++i){
            centroid[i] = Double.parseDouble(fields[i]);
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
        String outputPath = "hdfs://myc-ubuntu:9000/Lab2/cluster/KMedoide_Done";
        while(needLoop){
            // 创建Hadoop配置对象
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://myc-ubuntu:9000"); // 设置默认文件系统为HDFS
            serializeCentroids(conf);   // 读取并序列化中心点列表

            // 获取HDFS文件系统对象
            FileSystem fs = FileSystem.get(conf);

            // 删除上一次的作业
            Path outputDir = new Path(outputPath);
            if(fs.exists(outputDir)){
                fs.delete(outputDir, true);
            }
            fs.close();

            // 创建MapReduce作业
            Job job = Job.getInstance(conf, "KMedoide");

            // 指定Mapper类和Reducer类
            job.setMapperClass(KMedoide.KMedoideMapper.class);
            job.setReducerClass(KMedoide.KMedoideReducer.class);

            // 指定maptask的输出类型
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            // 指定reducetask的输出类型
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

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