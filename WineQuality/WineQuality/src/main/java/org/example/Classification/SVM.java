package org.example.Classification;

import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
* SVM类: 使用One-vs-All策略实现多分类，使用5折交叉验证评估模型的准确率
*/
public class SVM {
    private static String inputPath = "hdfs://node1:8020/wine/smote/smote_winequality-white.csv";

    public static void main(String[] args) throws Exception {
        // 打开文件写入准确率信息
        FileWriter fileWriter = new FileWriter("./accuracy-white.txt", true);    // 以追加模式打开

        // 初始化Spark
        SparkSession spark = SparkSession.builder()
                .appName("SVMTrain")
                .master("spark://node1:7077")
                .getOrCreate();

        // 加载数据
        Dataset<Row> ds = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(inputPath);

        // 获得属性名称数组
        List<String> allColumns = Arrays.asList(ds.columns());
        List<String> featureColumns = allColumns.stream().filter(col -> !"quality".equals(col)).collect(Collectors.toList());
        String[] featureNames = featureColumns.toArray(new String[0]);

        String labelName = "quality";

        // 将各个属性列组合成一个向量
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureNames)
                .setOutputCol("features");  // 向量名为features

        Dataset<Row> data = assembler.transform(ds);

        // 由于LinearSVCModel要求标签为Double类型，因此需要转换quality的类型
        data = data.withColumn("label", data.col(labelName).cast(DataTypes.DoubleType));

        // 将整个数据集随机划分为5个子集
        int k = 5;
        double[] foldWeights = new double[k];
        for(int i = 0; i < foldWeights.length; i++){
            foldWeights[i] = 1.0 / k;   // 设置每个折的权重，由于希望均分子集，所以每个折的权重都是1 / k
        }
        // 划分数据集
        Dataset<Row>[] folds = data.randomSplit(foldWeights, 1234);

        // 保存每个折的模型准确率
        List<Double> accuracies = new ArrayList<>();

        // k折交叉验证
        for(int i = 0; i < k; ++i)
        {
            fileWriter.write("验证集为第" + i + "个折\n");
            // 以第i个折为验证集，其它折为训练集
            Dataset<Row> validationSet = folds[i];

            // 合并训练集
            List<Row> emptyList = Collections.emptyList();
            Dataset<Row> trainSet = spark.createDataFrame(emptyList, data.schema());
            for(int j = 0; j < k; ++j)
            {
                if(j != i)
                {
                    trainSet = trainSet.union(folds[j]);
                }
            }

            // SVM训练和验证，使用One-vs-All策略
            // 注意到, 数据集中对于一部分类别是没有样本的（比如red中1~2，9~10），而SVM分类器是二分类的，样本中必须出现两类，需要排除这样的情况
            for(int label = 1; label <= 10; label++)
            {
                // 对当前类别建立SVM二分类模型，将样本中质量为当前类别的设置为1，其它类别设置为0. 注意标签的类型要为Double
                Dataset<Row> binaryTrainSet = trainSet.withColumn("binaryLabel", trainSet.col("label").equalTo(label).cast(DataTypes.DoubleType));

                // 排除只有一个类别(0)的训练集
                long distinctLabels = binaryTrainSet.select("binaryLabel").distinct().count();
                if(distinctLabels == 1)
                {
                    continue;
                }

                Dataset<Row> binaryValidationSet = validationSet.withColumn("binaryLabel", validationSet.col("label").equalTo(label).cast(DataTypes.DoubleType));

                // 计算样本数量，设置参数
                long count = binaryTrainSet.filter(binaryTrainSet.col("binaryLabel").equalTo(1)).count();

                // 大类别的参数
                int maxIter;
                double regParam;
                double tolerance;
                // 小类别的参数
                if(count <= 50)
                {
                    maxIter = 20;
                    regParam = 0.001;
                    tolerance = 1e-3;
                }
                else if(count <= 200)
                {
                    maxIter = 50;
                    regParam = 0.001;
                    tolerance = 1e-4;
                }else if(count <= 400)
                {
                    maxIter = 100;
                    regParam = 0.1;
                    tolerance = 1e-4;
                }else if(count <= 1000)
                {
                    maxIter = 400;
                    regParam = 0.5;
                    tolerance = 1e-4;
                }else if(count <= 2000)
                {
                    maxIter = 850;
                    regParam = 0.8;
                    tolerance = 1e-4;
                }else
                {
                    maxIter = 1500;
                    regParam = 0.9;
                    tolerance = 1e-4;
                }

                // 训练SVM模型
                LinearSVC lsvc = new LinearSVC()
                        .setLabelCol("binaryLabel") // 设置标签列
                        .setFeaturesCol("features") // 设置特征列(Vector类型)
                        .setTol(tolerance)
                        .setMaxIter(maxIter)            // 设置最大迭代次数
                        .setRegParam(regParam);          // 设置正则化参数, 防止模型过拟合（可能需要调整）

                LinearSVCModel model = lsvc.fit(binaryTrainSet);

                // 预测验证集
                Dataset<Row> predictions = model.transform(binaryValidationSet);
                long correctPredictions = predictions.filter(
                        predictions.col("binaryLabel").equalTo(predictions.col("prediction"))
                ).count();  // 预测正确的样本数
                long totalPredictions = predictions.count();

                // 计算当前SVM分类器的准确率
                double accuracy = correctPredictions * 1.0 / totalPredictions;
                accuracies.add(accuracy);
                fileWriter.write("类别" + label + " SVM分类器的准确率: " + accuracy + "\n");
            }
            fileWriter.write("==================================================\n");
        }

        // 输出整体平均准确率
        double averageAccuracy = accuracies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        fileWriter.write("整体平均准确率: " + averageAccuracy + "\n");

        // 打印到控制台
        System.out.print("\n");
        System.out.println("整体平均准确率: " + averageAccuracy);
        System.out.print("\n");
        try{
            Thread.sleep(2000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        // 用整个数据集训练SVM分类器，最后输出10个二分类SVM分类器，可以用于部署
        for(int label = 1; label <= 10; label++){
            // 训练的逻辑与k折交叉验证的训练逻辑是一样的，这里使用原始数据集data作为训练集
            Dataset<Row> binaryTrainSet = data.withColumn("binaryLabel", data.col("label").equalTo(label).cast(DataTypes.DoubleType));

            // 排除只有一个类别(0)的训练集
            long distinctLabels = binaryTrainSet.select("binaryLabel").distinct().count();
            if(distinctLabels == 1)
            {
                continue;
            }

            // 计算样本数量，设置参数
            long count = binaryTrainSet.filter(binaryTrainSet.col("binaryLabel").equalTo(1)).count();

            // 大类别的参数
            int maxIter = 100;
            double regParam = 0.1;
            double tolerance = 1e-4;
            // 小类别的参数
            if(count <= 200)
            {
                maxIter = 30;
                regParam = 0.01;
                tolerance = 1e-3;
            }

            // 训练SVM模型
            LinearSVC lsvc = new LinearSVC()
                    .setLabelCol("binaryLabel") // 设置标签列
                    .setFeaturesCol("features") // 设置特征列(Vector类型)
                    .setTol(tolerance)
                    .setMaxIter(maxIter)            // 设置最大迭代次数
                    .setRegParam(regParam);          // 设置正则化参数, 防止模型过拟合（可能需要调整）

            LinearSVCModel model = lsvc.fit(binaryTrainSet);

            // 保存到HDFS中
            String modelPath = "hdfs://node1:8020/wine/models/train-white-class-" + label;
            model.write().overwrite().save(modelPath);
        }

        // 关闭FileWriter
        fileWriter.close();

        // 关闭sparkSession
        spark.stop();
    }

}
