package org.example.Preproccessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.*;

/**
* PersonCorrelation: 计算11个属性与质量的皮尔森相关系数，筛选出相关系数最大的前7个属性
*/
public class PearsonCorrelation {
    private static String filePath = "hdfs://node1:8020/wine/winequality-white.csv";
    private static String outputPath = "hdfs://node1:8020/wine/new_winquality-white";

    public static void main(String[] args) throws IOException {
        // 删除上一次创建的文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");

        // 获取HDFS文件系统实例
        FileSystem fileSystem = FileSystem.get(conf);

        // 删除文件
        Path path = new Path(outputPath);

        if(fileSystem.exists(path))
        {
            fileSystem.delete(path);
        }

        // 初始化Spark
        SparkSession spark = SparkSession.builder()
                .appName("PearsonCorrelation")
                .master("spark://node1:7077")
                .getOrCreate();

        // 从filePath中加载数据集，创建Spark DataSet
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(filePath);

        // 构造属性列（不包含质量标签）
        List<String> features = Arrays.asList(
                "fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides",
                "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"
        );

        // 使用Correlation.corr方法计算各个属性与质量之间的皮尔森相关系数
        Map<String, Double> correlationMap = new HashMap<String, Double>(); // 存储各个属性与质量之间的皮尔森相关系数
        for(String feature : features) {
            Dataset<Row> correlationDS = df.select(feature, "quality");  // 提取属性名 和 quality列

            // 使用VectorAssembler.transform，将要计算相关系数的两列合并为一个向量，如1.2, 6合并为[1.2, 6]
            VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{feature, "quality"}).setOutputCol("combination"); // 工具类，指定输入两个列的列名和输出的列名
            Dataset<Row> combinedDS = vectorAssembler.transform(correlationDS);     // combinedDS中包含了feature、quality、[feature, quality]

            // 使用Correlation.corr方法计算feature与quality的皮尔森相关系数
            Row correlationRow = Correlation.corr(combinedDS, "combination", "pearson").head();   // 指定对"combination这个向量计算pearson相关系数
            Matrix correlationMatrix = correlationRow.getAs(0); // 获取相关性矩阵
            double correlation = correlationMatrix.apply(0, 1);   // 提取(0, 1)的指，即feature与quality的相关系数

            // 将相关系数保存到hashMap中
            correlationMap.put(feature, correlation);
        }

        // 打印各个属性与质量之间的相关系数
        for(Map.Entry<String, Double> entry : correlationMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        // 对哈希表按值排序
        List<Map.Entry<String, Double>> list = new ArrayList<>(correlationMap.entrySet());
        list.sort((entry1, entry2) -> Double.compare(Math.abs(entry2.getValue()), Math.abs(entry1.getValue())));    // 按照绝对值降序排列

        // 选择相关系数最大的前7个属性，并截取这7个属性作为新数据集
        String[] selectedColumns = new String[8];
        int i = 0;
        for(; i < 7; i++) {
            selectedColumns[i] = "`" + list.get(i).getKey() + "`";
        }
        selectedColumns[i] = "quality";

        // 打印选择的属性
        System.out.println("Selected columns: " + Arrays.toString(selectedColumns));

        // 通过select方法保留selectedColumns的列
        Dataset<Row> selectedDS = df.selectExpr(selectedColumns);
        selectedDS.write()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(outputPath);

        // 结束Spark会话
        spark.stop();
    }
}
