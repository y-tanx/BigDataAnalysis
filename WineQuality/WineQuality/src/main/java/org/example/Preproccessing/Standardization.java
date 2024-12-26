package org.example.Preproccessing;

import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
* Standardization类: 对数据集进行标准化，将各列转换为均值为0，方差为1的数据
*/
public class Standardization {
    private static String inputPath = "hdfs://node1:8020/wine/smote/smote_winequality-white.csv";
    private static String outputPath = "hdfs://node1:8020/wine/new_winequality-white";

    public static void main(String[] args) {
        // 初始化Spark
        SparkSession spark = SparkSession.builder()
                .appName("Standardization")
                .master("spark://node1:7077")
                .getOrCreate();

        // 从inputPath中加载数据集，创建Spark Dataset
        Dataset<Row> ds = spark.read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv(inputPath);

        // 构造属性名数组，使用流处理
        List<String> allColumns = Arrays.asList(ds.columns());
        List<String> featureColumns = allColumns.stream().filter(col -> !"quality".equals(col)).collect(Collectors.toList());
        String[] features = featureColumns.stream().toArray(String[]::new);

        // 使用VectorAssembler将特征组合成一个向量
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(features)    // 注意接收的是String数组
                .setOutputCol("originalFeatures");   // 合并后的向量命名为originalFeatures

        Dataset<Row> assembledDS = vectorAssembler.transform(ds);

        // 使用StandardScaler对特征向量进行标准化
        StandardScaler scaler = new StandardScaler()
                .setInputCol("originalFeatures")    // 输入的向量名为originalfeatures，即上一步合并出的向量
                .setOutputCol("standardFeatures")   // 输出的向量名为standardFeatures
                .setWithMean(true)
                .setWithStd(true);   // 将数据均值标准化为0，方差标准化为1

        Dataset<Row> standardDS = scaler.fit(assembledDS).transform(assembledDS);
        standardDS.show();

        // 将标准化后的向量列拆解成单个属性列
        Dataset<Row> resultDS = standardDS;
        for(int i = 0; i < features.length; i++){
            resultDS = resultDS.drop(features[i]);  // 删除原始列
            String columnName = "`" + features[i] + "`";    // 注意到有空格需要加上``
            // 从向量中分解出第i个属性的列
            int finalI = i;
            resultDS = resultDS.withColumn(columnName, functions.udf(
                    (Vector v) -> v.toArray()[finalI], DataTypes.DoubleType
                    ).apply(functions.col("standardFeatures")));
        }

        // 构造结果DS，注意到resultDS中包含了各个属性列 + standardFeatures + originalFeatures列，需要删除多余的列
        Dataset<Row> outputDS = resultDS.drop("standardFeatures").drop("originalFeatures");

        // 写入HDFS
        outputDS.write()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(outputPath);

        // 结束Spark会话
        spark.stop();
    }
}
