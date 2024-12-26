# 实验一 数据预处理

## 实验要求

掌握数据预处理的步骤和方法，包括数据抽样、数据过滤、数据标准化和归一化、数据清洗，理解数据预处理各个步骤在大数据环境下的实现方式

## 数据集概述

本次实验的数据是旅行者评价数据集，每行为一条数据，不同列之间使用"|"符号分隔，需要处理的属性：

1. 分层抽样：user_career(10)
2. 奇异值过滤：longitude(1)和latitude(2)，边界给出了
3. 数据格式属性：user_birthday(8)和review_date(4); temperature(5)
4. 归一化：rating(6)
5. 缺失值属性：rating(6)和user_income(11),在文件中以？占位

- rating依赖于user_income(11)、longtitude(1)、latitude(2)和altitude(3)
- user_income依赖于user_nationality(9)和user_career(10)

## 分层抽样

+ **Mapper：从输入数据中提取职业字段，并将数据根据职业字段进行分组。每个组由一个职业及其对应的所有记录组成**
  1. **分割输入数据**： 每行数据使用 `|` 分隔成多个字段，提取出职业字段（`career`）作为分组的依据。职业字段位于第11个字段（索引为10）
  2. **生成键值对**： 对于每一行数据，若字段数量超过10个（确保包含职业字段），则输出一个键值对：键为职业（`career` 字段），值为整行数据。通过 `context.write(new Text(career), value)` 输出
+ **Reducer：对每个职业类别（每个 `career`）的记录进行抽样，随机抽取20%的数据，并将结果输出**
  1. **存储记录**： 将Mapper传递过来的每个职业组的记录存储到 `List<Text>` 中。每个职业类别对应一组记录
  2. **计算抽样数量**： 设定抽样比例为20%（`sampleRate = 0.2`）。根据记录数量计算出需要抽取的数量
  3. **进行随机抽样**： 使用 `Random` 类随机抽取记录。为了避免重复抽样，每抽取一条记录，就从 `records` 列表中移除该记录

## 奇异值过滤

+ **Mapper：从输入数据中提取longitude(1)和latitude(2)，按照给定的边界，将满足边界范围的样本输出到上下文**
+ **Reducer：直接输出所有的样本**

## 数据格式属性和归一化

+ **Mapper**

  + map阶段提取出user_birthday、review_date、temperature、rating

  + 日期格式化：使用`formatDate`方法将日期字符串解析为不同的格式，并将其转换为 `yyyy-mm-dd` 格式。`formatDate`方法中使用了SimpleDateFormat类对日期字符串进行匹配，并将格式转换为yyyy-mm-dd格式：

    ```java
    SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd");
    ```

  + 温度格式化：使用`formatTemperature(temperature)`将温度字段从华氏度（如果有）转换为摄氏度，或者如果已经是摄氏度，则直接使用

  + 替换原始字段：使用String.replace()方法用标准化后的日期和温度替换原始的字段。

  + 最终将处理后的行（标准化后的数据）输出到MapReduce的上下文。

+ **Reducer**

  + setup阶段计算 `ratingRange = ratingMax - ratingMin`，即评分的极差。此值会用于归一化过程
  + reduce阶段对评分归一化。在获得rating字段的值后，通过公式 `(rating - ratingMin) / ratingRange` 对评分进行归一化，将评分映射到一个0到1之间的值。最后将归一化后的评分替换原始的评分，并格式化为两位小数：`String normalizedRating = String.format("%.2f", normalizedRatingValue)`
  + 最终将归一化后的记录输出到Reduce的上下文

## 缺失值填充

### 相关性的使用策略

采用热卡插值策略，这实际上是K=1的KNN插值，选择与缺失值样本最相似的记录，用它的平均值填充到缺失的属性中。考虑到user_income与user_nationality和user_career相关，rating与user_income(11)、longtitude(1)、latitude(2)和altitude(3)相关，因此设计一种“组合键“，组合键中包含了所有与缺失属性相关的属性，用这个组合键进行热卡插值

### 实现思路

需要两轮MapReduce，这是因为rating与缺失属性user_income相关，因此需要先将user_income的缺失值填充完，再对rating进行填充

1. 第一轮MapReduce：对user_income进行插值
   + Mapper阶段：定义一个哈希表，以user_nationality和user_career的组合作为键值对，记录<键值对，income属性和与总数>
   + Reduce阶段：对于每一个缺失值记录，从前到后遍历哈希表，如果user_nationality/user_career相同，则+1。记录相似度最高的键值对（键值对也需要用一个类来构建，因为我们要拆分出单个属性作比较），用相似度最高的键值对的平均值进行填充
2. 第二轮MapReduce：对rating进行插值
   + Mapper阶段：定义一个哈希表，以user_income(11)、longtitude(1)、latitude(2)和altitude(3)的组合作为键值对，记录<键值对，rating属性和与总数>，同时统计这四个属性的最大/最小值，为归一化做准备
   + Reducer阶段：对于每一个缺失值记录，从前到后遍历哈希表，在归一化的同时计算欧式距离（这很难算，说实在的），选择欧式距离最小的，用它的平均值作为我们缺失属性的值



























