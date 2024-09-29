package com.org.Imputation;

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
import java.util.HashMap;

/**
 * 本类实现对rating属性的缺失值填充
 *
 * @author myc
 * @version 1.0
 */
public class RatingImputation {
    private static HashMap<RatingDependices, Record> ratingMap = new HashMap<>();
    // 属性值的Max/Min
    private static double[] incomeMaxAndMin = {Double.MIN_VALUE, Double.MAX_VALUE};
    private static double[] longitudeMaxAndMin = {Double.MIN_VALUE, Double.MAX_VALUE};
    private static double[] latitudeMaxAndMin = {Double.MIN_VALUE, Double.MAX_VALUE};
    private static double[] altitudeMaxAndMin = {Double.MIN_VALUE, Double.MAX_VALUE};
    // 属性值极差
    private static double incomeRange = 1;
    private static double longitudeRange = 1;
    private static double latitudeRange = 1;
    private static double altitudeRange = 1;

    /**
     * Mapper类：统计每种rating依赖属性组合中，记录的数量和所有记录中rating之和
     */
    public static class RatingImputationMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * map函数：构造映射<rating的依赖属性组合，记录的数量和所有记录中rating之和>
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常*
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\|");
            // 获取rating字段
            String rating = fields[6];
            // 获得rating的依赖属性字段
            String income = fields[11];
            String longitude = fields[1];
            String latitude = fields[2];
            String altitude = fields[3];
            // Mapper对rating未缺失的记录进行统计
            if(rating.charAt(0) != '?')
            {
                RatingDependices ratingDependices = new RatingDependices(income, longitude, latitude, altitude);
                // 从哈希表中查找该依赖属性组合
                Record record = ratingMap.get(ratingDependices);
                if(record != null)
                {
                    // 如果哈希表中有这一种依赖属性组合，则更新信息
                    record.setNum(record.getNum() + 1);
                    record.setSum(record.getSum() + Double.parseDouble(rating));
                }else
                {
                    // 如果哈希表中没有这一种依赖属性组合，则创建这个映射，添加到哈希表中
                    record = new Record(1, Double.parseDouble(rating));
                    ratingMap.put(ratingDependices, record);
                }
            }
            // 更新依赖属性组合的最大/最小值
            UpdateMaxAndMin(income, longitude, latitude, altitude);
            // 输出到上下文
            context.write(new Text(line), new Text(""));
        }

        /**
         * 在Mapper结束后，计算每个依赖属性的极差
         *
         * @param context 上下文对象
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 计算极差
            incomeRange = incomeMaxAndMin[0] - incomeMaxAndMin[1];
            longitudeRange = longitudeMaxAndMin[0] - longitudeMaxAndMin[1];
            latitudeRange = latitudeMaxAndMin[0] - latitudeMaxAndMin[1];
            altitudeRange = altitudeMaxAndMin[0] - altitudeMaxAndMin[1];
        }

    }
    /**
     * Reducer类：用热卡插值方法填充rating的缺失值
     */
    public static class RatingImputationReducer extends Reducer<Text, Text, Text, Text>
    {
        /**
         * 对rating依赖属性进行归一化，用欧式距离衡量相似度，选择相似度最大的依赖属性组合，用该依赖属性组合的rating的平均值填充缺失的rating
         *
         * @param key Mapper输入的career字段
         * @param values 对应职业的记录
         * @param context 上下文对象，用于输出抽样后的记录
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] fields = line.split("\\|");
            double distanceMin = Double.MAX_VALUE;
            RatingDependices bestDependices = null;
            // 获取rating字段
            String rating = fields[6];
            // Reducer对含有缺失rating的记录做处理
            if(rating.charAt(0) == '?')
            {
                double distance = 0;
                // 获得依赖属性的最小值
                double incomeMin = incomeMaxAndMin[1];
                double longitudeMin = longitudeMaxAndMin[1];
                double latitudeMin = latitudeMaxAndMin[1];
                double altitudeMin = altitudeMaxAndMin[1];
                // 获得rating的依赖属性字段归一化的结果
                double income = (Double.parseDouble(fields[11]) - incomeMin) / incomeRange;
                double longitude = (Double.parseDouble(fields[1]) - longitudeMin) /longitudeRange;
                double latitude = (Double.parseDouble(fields[2]) - latitudeMin) / latitudeRange;
                double altitude = (Double.parseDouble(fields[3]) - altitudeMin) / altitudeRange;
                // 遍历哈希表，对属性进行归一化，与每个依赖属性组合计算欧式距离
                for(RatingDependices dependices : ratingMap.keySet()) {
                    double map_income = (Double.parseDouble(dependices.getIncome()) - incomeMin) / incomeRange;
                    double map_longitude = (Double.parseDouble(dependices.getLongitude()) - longitudeMin) / longitudeRange;
                    double map_latitude = (Double.parseDouble(dependices.getLatitude()) - latitudeMin) / latitudeRange;
                    double map_altitude = (Double.parseDouble(dependices.getAltitude()) - altitudeMin) / altitudeRange;
                    // 计算相似度
                    double sum = Math.pow(map_income - income, 2)
                            + Math.pow(map_longitude - longitude, 2)
                            + Math.pow(map_latitude - latitude, 2)
                            + Math.pow(map_altitude - altitude, 2);
                    distance = Math.sqrt(sum);
                    // 记录相似度最大的一组属性组合,即距离最小的一组属性
                    if(distance < distanceMin)
                    {
                        distanceMin = distance;
                        bestDependices = dependices;
                    }
                }
                if(bestDependices != null)
                {
                    Record bestRecord = ratingMap.get(bestDependices);
                    double average = bestRecord.getSum() / bestRecord.getNum();
                    line = line.replace(rating, String.format("%.2f", average));
                }
            }
            // 输出到上下文
            context.write(new Text(line), new Text(""));
        }
    }

    /**
     * 更新当前的依赖属性组合的最大/最小值，为后续归一化做准备
     *
     * @param income        当前记录中用户的收入
     * @param longitude     当前记录中用户的经度
     * @param latitude      当前记录中用户的维度
     * @param altitude      当前记录中用户所处的海拔高度
     */
    public static void UpdateMaxAndMin(String income, String longitude, String latitude, String altitude)  {
        double incomeValue = Double.parseDouble(income);
        double longitudeValue = Double.parseDouble(longitude);
        double latitudeValue = Double.parseDouble(latitude);
        double altitudeValue = Double.parseDouble(altitude);
        // 比较当前记录中字段的值和当前统计的最大/最小值
        incomeMaxAndMin[0] = Math.max(incomeValue, incomeMaxAndMin[0]);
        incomeMaxAndMin[1] = Math.min(incomeValue, incomeMaxAndMin[1]);
        longitudeMaxAndMin[0] = Math.max(longitudeValue, longitudeMaxAndMin[0]);
        longitudeMaxAndMin[1] = Math.min(longitudeValue, longitudeMaxAndMin[1]);
        latitudeMaxAndMin[0] = Math.max(latitudeValue, latitudeMaxAndMin[0]);
        latitudeMaxAndMin[1] = Math.min(latitudeValue, latitudeMaxAndMin[1]);
        altitudeMaxAndMin[0] = Math.max(altitudeValue, altitudeMaxAndMin[0]);
        altitudeMaxAndMin[1] = Math.max(altitudeValue, altitudeMaxAndMin[1]);
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
        String inputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Done";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Done2";
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
        Job job = Job.getInstance(conf, "RatingImputation");
        // 指定Mapper类和Reducer类
        job.setMapperClass(RatingImputation.RatingImputationMapper.class);
        job.setReducerClass(RatingImputation.RatingImputationReducer.class);
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

/**
 * 记录Rating的依赖属性组合
 */
class RatingDependices
{
    String income;
    String longitude;
    String latitude;
    String altitude;

    // RatingDependices的构造函数
    public RatingDependices(String income, String longitude, String latitude, String altitude)
    {
        this.income = income;
        this.longitude = longitude;
        this.latitude = latitude;
        this.altitude = altitude;
    }

    public String getIncome()
    {
        return this.income;
    }

    public String getLongitude()
    {
        return this.longitude;
    }

    public String getLatitude()
    {
        return this.latitude;
    }
    public String getAltitude()
    {
        return this.altitude;
    }
}
