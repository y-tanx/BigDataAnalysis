# BigDataAnalysis
哈工大大数据分析课程实验

## 实验

开发环境：Ubuntu22.04 Hadoop伪分布式

开发语言：Java

+ lab1：数据预处理
+ lab2：分类和聚类
+ lab3：图数据分析

实验报告见`labxx/report`

## 大作业

开发环境：Ubuntu22.04 Spark Standalone + HDFS

开发语言：Java

本实验使用3个虚拟机搭建集群，实现Spark Standalone**计算框架** + HDFS **分布式文件系统**。这需要搭建Spark集群 和 Hadoop完全分布式集群，计算框架使用Spark，文件系统使用HDFS。3台主机在Spark集群和Hadoop集群的角色如下所示：

| 节点名（主机名） | Spark集群角色 |                        Hadoop集群角色                        |
| :--------------: | :-----------: | :----------------------------------------------------------: |
|      node1       |    Master     | ResourceManager、NodeManager（资源管理）、NameNode、DataNode（存储管理） |
|      node2       |    Worker     | NodeManager（资源管理）、Secondary NameNode、DataNode（存储管理） |
|      node3       |    Worker     |        NodeManager（资源管理）、DataNode（存储管理）         |

在启动集群时，先启动HDFS文件系统：`start-dfs.sh`，再启动Spark集群：`start-all.sh`

大作业的数据来源：https://archive.ics.uci.edu/datasets/

可以考虑使用PySpark，Python数据分析更方便









