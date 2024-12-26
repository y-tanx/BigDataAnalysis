package com.org.FormatAndNormalize;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 这个类实现了对日期，温度的标准化，对rating的归一化
 * @author myc
 * @version 2.0
 */
public class FormatAndNormalize {
    private static double ratingMax = Double.NEGATIVE_INFINITY;    // rating的最大值
    private static double ratingMin = Double.POSITIVE_INFINITY;    // rating的最小值
    private static double ratingRange;

    /**
     * Mapper类：将日期转换为yyyy-mm-dd的格式；将温度转换为摄氏度，更新rating的min和max值
     * 输出的键值对为：<记录，“”>
     */
    public static class FormatAndNormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * 处理每一行数据，将文本行中日期和温度字段规格化，同时记录rating的min/max值
         *
         * @param key 当前行的偏移量
         * @param value 输入的文本行
         * @param context MapReduce的上下文
         * @throws IOException 文件IO时可能出现的异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\|");
            // 规格化
            //  user_birthday(8)，review_date(4)，temperature(5),rating(6)
            String userBirthday = fields[8];
            String reviewDate = fields[4];
            String temperature = fields[5];
            String rating = fields[6];

            // 对日期规格化
            String formattedBirthday = formatDate(userBirthday);
            String formattedReviewDate = formatDate(reviewDate);
            // 对温度规格化
            String formattedTemperature = formatTemperature(temperature);
            // 替换value中的日期，温度字段
            line = line.replace(userBirthday, formattedBirthday)
                        .replace(reviewDate, formattedReviewDate)
                        .replace(temperature, formattedTemperature);
            // 输出到上下文
            context.write(new Text(line), new Text(""));
            // 归一化
            // 比较这一行的rating和当前的ratingMin/ratingMax，更新rating的min/max
            // 注意rating有缺失值，用“？”占位
            if(!("?".equals(rating)))
            {
                double ratingValue = Double.parseDouble(rating);
                ratingMin = Math.min(ratingMin, ratingValue);
                ratingMax = Math.max(ratingMax, ratingValue);
            }
        }
    }

    /**
     * Reducer类：对rating进行归一化：(rating-ratingMin)/(ratingMax-ratingMin)
     */
    public static class FormatAndNormalizeReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 计算rating的极差，由于每次Reduce都会用到极差，因此写到setup方法中
         *
         * @param context MapReduce上下文对象
         *
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            ratingRange = ratingMax - ratingMin;
        }

        /**
         * 处理Mapper阶段分组后的数据，对rating字段归一化
         *
         * @param key Mapper输入的career字段
         * @param values 对应职业的记录
         * @param context 上下文对象，用于输出抽样后的记录
         * @throws java.io.IOException 处理文件时抛出的IO异常
         * @throws InterruptedException MapReduce任务中断时抛出的异常
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] fields = line.split("\\|");
            String rating = fields[6];
            // 对rating进行归一化
            if(!("?".equals(rating)))
            {
                double normalizedRatingValue = Double.parseDouble(rating) / ratingRange;
                String normalizedRating = String.format("%.2f", normalizedRatingValue);
                line = line.replace(rating, normalizedRating);
            }
            // 输出到上下文
            context.write(new Text(line), new Text(""));
        }
    }

    /**
     * 将日期格式转换为yyyy-mm-dd的格式
     *
     * @param dateStr 文本行中的日期字符串
     * @return 格式为yyyy-mm-dd的dataStr字符串
     *
     */
    private static String formatDate(String dateStr){
        Date date = null;
        String[] formats =
                {
                        "yyyy-MM-dd",
                        "yyyy/MM/dd",
                        "MMMM d,yyyy",
                        "MMMM dd,yyyy"
                };
        // 首先，尝试用每种格式解析日期，将日期信息存放到date中
        for(String format: formats)
        {
            try{
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                 date = sdf.parse(dateStr);
                break;  // 如果成功解析，则跳出循环
            }catch (ParseException e)
            {
                // 继续尝试用下一种格式解析
            }
        }
        // 如果成功匹配上了某种格式，则进行格式化输出
        if(date != null)
        {
            SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd");
            return targetFormat.format(date);
        }
        return null;
    }
    /**
     * 将温度统一成摄氏度
     *
     * @param temperature 文本行中的温度，摄氏度或华氏度
     * @return 转化为摄氏度的温度
     */
    private static String formatTemperature(String temperature)
    {
        String normalizeTem = null;
        // 判断temperature是华氏度还是摄氏度
        if(temperature.contains("℉"))
        {
            // 是华氏度，需要转换为摄氏度
            // 首先获取温度数值
            String temValue = temperature.replace("℉","");
            // 转换为摄氏度数值，并规格化输出
            double celsius = (5.0 / 9.0) * (Double.parseDouble(temValue) - 32);
            normalizeTem = String.format("%.1f℃", celsius);
        }else if(temperature.contains("℃"))
        {
            // 是摄氏度，直接输出
            normalizeTem = temperature;
        }
        return normalizeTem;
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
        String inputPath = "hdfs://myc-ubuntu:9000/Lab1/D_Filter";
        String outputPath = "hdfs://myc-ubuntu:9000/Lab1/D_FormatAndNormalize";
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
        Job job = Job.getInstance(conf, "FormatAndNormalize");
        // 指定Mapper类和Reducer类
        job.setMapperClass(FormatAndNormalizeMapper.class);
        job.setReducerClass(FormatAndNormalizeReducer.class);
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